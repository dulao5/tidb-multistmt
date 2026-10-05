package multistmt

import (
	"container/list"
	"database/sql"
	"fmt"
	"strings"
	"sync"
)

// PreparedCache transparently manages server-side PREPARE reuse across
// Batch.Execute calls (via WithPreparedCache), for every statement that
// doesn't set Statement.PreparedName itself.
//
// Why this needs its own cache instead of reusing database/sql's: a single
// repeated statement is already handled for free by (*sql.DB).Prepare's
// returned *sql.Stmt, which tracks per-physical-connection "have I already
// PREPAREd myself there" state internally (the unexported css field).
// Higher-level wrappers like GORM's PreparedStmtDB don't reinvent that —
// they're really just an LRU of *sql.Stmt keyed by SQL text, leaning on
// *sql.Stmt's own per-connection bookkeeping underneath. multistmt can't
// lean on it the same way: a batch is sent as one hand-built multi-statement
// string (PREPARE/SET/EXECUTE/DEALLOCATE), never through
// (*sql.DB).PrepareContext, so nothing tracks "is this SQL text already
// PREPAREd on this connection" unless this package does it itself.
//
// The identity problem: tracking must be keyed by the *physical* connection,
// not by the *sql.Conn Go value — database/sql hands out a fresh *sql.Conn
// wrapper on every pool checkout even when the underlying physical
// connection (and its TiDB session, with whatever is still PREPAREd on it)
// is the same one as last time. Keying by the *sql.Conn pointer would make
// PreparedCache re-PREPARE needlessly on every checkout; PreparedCache
// instead identifies the physical connection via (*sql.Conn).Raw, so a
// statement already PREPAREd in a session is recognized and reused even
// after that connection cycled back through the pool in between.
//
// A PreparedCache is safe for concurrent use by multiple goroutines, though
// in practice each physical connection is only ever driven by one goroutine
// at a time (that's what pinning a *sql.Conn means).
type PreparedCache struct {
	maxStmtsPerConn int
	maxConns        int

	mu    sync.Mutex
	conns map[any]*connCache // key: identity from connIdentity
	order *list.List         // outer LRU of *connCache; front = most recently used
}

// NewPreparedCache creates a PreparedCache.
//
// maxStmtsPerConn caps how many distinct SQL texts PreparedCache keeps
// PREPAREd on any one physical connection at a time; past that, the
// least-recently-used one is DEALLOCATEd (in the same round trip as
// whatever new statement needed the room) to make space. maxConns caps how
// many distinct physical connections PreparedCache tracks at all; past
// that, the oldest connection's bookkeeping is simply forgotten — not
// DEALLOCATEd, since PreparedCache no longer holds that connection to send
// it on. That connection's TiDB session may still have those statements
// PREPAREd; losing track just costs one redundant PREPARE the next time
// PreparedCache sees it, not a correctness problem.
//
// Non-positive values fall back to defaults (32 statements per connection,
// 256 connections).
func NewPreparedCache(maxStmtsPerConn, maxConns int) *PreparedCache {
	if maxStmtsPerConn <= 0 {
		maxStmtsPerConn = 32
	}
	if maxConns <= 0 {
		maxConns = 256
	}
	return &PreparedCache{
		maxStmtsPerConn: maxStmtsPerConn,
		maxConns:        maxConns,
		conns:           make(map[any]*connCache),
		order:           list.New(),
	}
}

type connCache struct {
	key     any
	stmts   map[string]*list.Element // SQL text -> element in order (Value is *stmtEntry)
	order   *list.List               // inner LRU of this connection's PREPAREd statements
	lruElem *list.Element            // this connCache's own element in PreparedCache.order
}

type stmtEntry struct {
	sql  string
	name string
}

// connIdentity returns a key that is stable for the same physical connection
// across repeated database/sql pool checkouts — unlike *conn itself, which
// is a fresh wrapper object every time (*sql.DB).Conn is called, even when
// the pool hands back the same underlying connection.
//
// It works by reaching through to the driver.Conn (*sql.Conn).Raw exposes:
// that value's dynamic type is a pointer into the driver (e.g.
// go-sql-driver/mysql's *mysqlConn), stable for the physical connection's
// whole lifetime, so it compares equal (and hashes consistently as a map
// key) across checkouts of the same connection and only that connection.
func connIdentity(conn *sql.Conn) (any, error) {
	var id any
	err := conn.Raw(func(driverConn any) error {
		id = driverConn
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("multistmt: prepared cache: cannot access underlying driver connection: %w", err)
	}
	return id, nil
}

// planItem is PreparedCache's decision for one statement in a batch.
type planItem struct {
	name    string
	skip    bool // true: already PREPAREd on this connection (or earlier in this same batch); omit PREPARE
	managed bool // true: PreparedCache owns commit bookkeeping for this statement
	isMiss  bool // true: name was newly minted this round; only persist on confirmed success
}

// plan decides the PreparedName/SkipPrepare to use for every statement in
// stmts, and returns the names of any previously-cached entries evicted to
// make room for new ones — the caller must DEALLOCATE those names in the
// very same round trip (Batch.build's extraDealloc parameter does this),
// since this is the only time PreparedCache will hold that physical
// connection to send them on.
func (c *PreparedCache) plan(connID any, stmts []Statement) ([]planItem, []string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	cc := c.getOrCreateConnLocked(connID)

	plans := make([]planItem, len(stmts))
	var evicted []string
	local := make(map[string]int) // SQL text -> index of its first (miss) occurrence in this batch

	for i, s := range stmts {
		if s.PreparedName != "" {
			// Caller is managing this statement's PREPARE lifecycle itself;
			// PreparedCache leaves it alone entirely.
			plans[i] = planItem{name: s.PreparedName, skip: s.SkipPrepare}
			continue
		}

		if elem, ok := cc.stmts[s.SQL]; ok {
			cc.order.MoveToFront(elem)
			plans[i] = planItem{name: elem.Value.(*stmtEntry).name, skip: true, managed: true}
			continue
		}

		if firstIdx, ok := local[s.SQL]; ok {
			// Same SQL text appears earlier in this same batch as a miss —
			// its PREPARE will already have run by the time this later
			// EXECUTE reaches the server, so this occurrence can reuse the
			// name without its own PREPARE too.
			plans[i] = planItem{name: plans[firstIdx].name, skip: true, managed: true}
			continue
		}

		for len(cc.stmts) >= c.maxStmtsPerConn {
			tail := cc.order.Back()
			if tail == nil {
				break
			}
			ent := tail.Value.(*stmtEntry)
			delete(cc.stmts, ent.sql)
			cc.order.Remove(tail)
			evicted = append(evicted, ent.name)
		}

		local[s.SQL] = i
		plans[i] = planItem{name: nextGeneratedName(), skip: false, managed: true, isMiss: true}
	}

	return plans, evicted
}

// commit records the outcome of a planned batch: statements that were newly
// PREPAREd this round (isMiss) and confirmed to succeed are added to the
// per-connection cache; everything else is left as-is. outcomes[i] is the
// error delivered to statement i's Callback (nil means success).
func (c *PreparedCache) commit(connID any, stmts []Statement, plans []planItem, outcomes []error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	cc, ok := c.conns[connID]
	if !ok {
		// Forgotten by the outer connection LRU between plan and commit;
		// nothing to update.
		return
	}

	for i, p := range plans {
		if !p.managed {
			continue
		}
		if isSessionDesyncError(outcomes[i]) {
			c.dropConnLocked(connID)
			return
		}
		if !p.isMiss {
			continue
		}
		if outcomes[i] != nil {
			// Never confirmed PREPAREd (the statement failed, or an earlier
			// one in the batch did and this one was skipped) — don't cache
			// a name that may not actually exist server-side.
			continue
		}
		if _, exists := cc.stmts[stmts[i].SQL]; exists {
			continue
		}
		ent := &stmtEntry{sql: stmts[i].SQL, name: p.name}
		cc.stmts[stmts[i].SQL] = cc.order.PushFront(ent)
	}
}

func (c *PreparedCache) getOrCreateConnLocked(connID any) *connCache {
	if cc, ok := c.conns[connID]; ok {
		c.order.MoveToFront(cc.lruElem)
		return cc
	}

	for len(c.conns) >= c.maxConns {
		back := c.order.Back()
		if back == nil {
			break
		}
		delete(c.conns, back.Value.(*connCache).key)
		c.order.Remove(back)
	}

	cc := &connCache{
		key:   connID,
		stmts: make(map[string]*list.Element),
		order: list.New(),
	}
	cc.lruElem = c.order.PushFront(cc)
	c.conns[connID] = cc
	return cc
}

func (c *PreparedCache) dropConnLocked(connID any) {
	if cc, ok := c.conns[connID]; ok {
		delete(c.conns, connID)
		c.order.Remove(cc.lruElem)
	}
}

// isSessionDesyncError reports whether err looks like the server no longer
// has a prepared statement PreparedCache believes is live — e.g. the
// session was reset, or the physical connection died and was silently
// replaced underneath an identity PreparedCache had on file. When this
// happens PreparedCache forgets everything it knew about that connection
// rather than risk repeating the same error on every subsequent batch.
func isSessionDesyncError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unknown prepared statement") ||
		strings.Contains(msg, "bad connection")
}
