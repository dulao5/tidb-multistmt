package multistmt

import (
	"errors"
	"testing"
)

func TestCache_SecondPlanHitsAfterCommit(t *testing.T) {
	c := NewPreparedCache(0, 0)
	connA := "conn-a"
	stmts := []Statement{{SQL: "SELECT 1"}}

	plans, evicted := c.plan(connA, stmts)
	if len(evicted) != 0 {
		t.Fatalf("expected no evictions on first plan, got %v", evicted)
	}
	if !plans[0].isMiss || plans[0].skip {
		t.Fatalf("expected a fresh miss on first plan, got %+v", plans[0])
	}

	c.commit(connA, stmts, plans, []error{nil})

	plans2, _ := c.plan(connA, stmts)
	if !plans2[0].skip {
		t.Fatalf("expected a cache hit (skip=true) after a successful commit, got %+v", plans2[0])
	}
	if plans2[0].name != plans[0].name {
		t.Fatalf("expected the same PREPARE name to be reused, got %q then %q", plans[0].name, plans2[0].name)
	}
}

func TestCache_FailedCommitDoesNotCache(t *testing.T) {
	c := NewPreparedCache(0, 0)
	connA := "conn-a"
	stmts := []Statement{{SQL: "SELECT 1"}}

	plans, _ := c.plan(connA, stmts)
	c.commit(connA, stmts, plans, []error{errors.New("syntax error")})

	plans2, _ := c.plan(connA, stmts)
	if !plans2[0].isMiss || plans2[0].skip {
		t.Fatalf("a statement whose PREPARE failed must not be cached, got %+v", plans2[0])
	}
}

func TestCache_DuplicateSQLWithinOneBatchSharesOnePrepare(t *testing.T) {
	c := NewPreparedCache(0, 0)
	connA := "conn-a"
	stmts := []Statement{{SQL: "SELECT 1"}, {SQL: "SELECT 1"}, {SQL: "SELECT 2"}}

	plans, _ := c.plan(connA, stmts)
	if plans[0].skip {
		t.Fatalf("first occurrence must still PREPARE, got %+v", plans[0])
	}
	if !plans[1].skip || plans[1].name != plans[0].name {
		t.Fatalf("second occurrence of the same SQL in-batch must reuse the first's name and skip PREPARE, got %+v (want name %q)", plans[1], plans[0].name)
	}
	if plans[2].skip || plans[2].name == plans[0].name {
		t.Fatalf("a different SQL text must get its own fresh name, got %+v", plans[2])
	}
}

func TestCache_PerConnEvictionDeallocatesLRU(t *testing.T) {
	c := NewPreparedCache(1, 0) // only 1 statement cached per connection
	connA := "conn-a"

	s1 := []Statement{{SQL: "SELECT 1"}}
	plans1, evicted1 := c.plan(connA, s1)
	if len(evicted1) != 0 {
		t.Fatalf("expected no eviction when cache isn't full yet, got %v", evicted1)
	}
	c.commit(connA, s1, plans1, []error{nil})

	s2 := []Statement{{SQL: "SELECT 2"}}
	plans2, evicted2 := c.plan(connA, s2)
	if len(evicted2) != 1 || evicted2[0] != plans1[0].name {
		t.Fatalf("expected statement 1's name (%q) to be evicted to make room, got %v", plans1[0].name, evicted2)
	}
	c.commit(connA, s2, plans2, []error{nil})

	// statement 1 must now be a miss again — it was evicted.
	plans1Again, _ := c.plan(connA, s1)
	if !plans1Again[0].isMiss {
		t.Fatalf("expected statement 1 to be a fresh miss after eviction, got %+v", plans1Again[0])
	}
}

func TestCache_OuterConnLRUForgetsOldestConnectionWithoutDeallocating(t *testing.T) {
	c := NewPreparedCache(0, 1) // only 1 connection tracked at all
	connA, connB := "conn-a", "conn-b"

	sA := []Statement{{SQL: "SELECT 1"}}
	plansA, _ := c.plan(connA, sA)
	c.commit(connA, sA, plansA, []error{nil})

	// Touching connB evicts connA's bookkeeping entirely (no DEALLOCATE
	// names returned: Cache no longer holds connA to send them on).
	sB := []Statement{{SQL: "SELECT 2"}}
	plansB, evictedB := c.plan(connB, sB)
	if len(evictedB) != 0 {
		t.Fatalf("outer connection eviction must not report statement-level DEALLOCATEs, got %v", evictedB)
	}
	c.commit(connB, sB, plansB, []error{nil})

	plansAAgain, _ := c.plan(connA, sA)
	if !plansAAgain[0].isMiss {
		t.Fatalf("expected conn A's cache entry to have been forgotten, got %+v", plansAAgain[0])
	}
}

func TestCache_SessionDesyncErrorDropsConnection(t *testing.T) {
	c := NewPreparedCache(0, 0)
	connA := "conn-a"
	stmts := []Statement{{SQL: "SELECT 1"}}

	plans, _ := c.plan(connA, stmts)
	c.commit(connA, stmts, plans, []error{nil})

	// A later, unrelated batch on the same connection hits a desync error
	// for some other statement.
	stmts2 := []Statement{{SQL: "SELECT 2"}}
	plans2, _ := c.plan(connA, stmts2)
	c.commit(connA, stmts2, plans2, []error{errors.New("Unknown prepared statement handler given to EXECUTE")})

	// Both the original cached statement and the failed one should now be
	// gone — the whole connection's bookkeeping was dropped.
	plans1Again, _ := c.plan(connA, stmts)
	if !plans1Again[0].isMiss {
		t.Fatalf("expected statement 1 to be re-PREPAREd after a session desync drop, got %+v", plans1Again[0])
	}
}

func TestCache_ManualPreparedNameBypassesCache(t *testing.T) {
	c := NewPreparedCache(0, 0)
	connA := "conn-a"
	stmts := []Statement{{SQL: "SELECT 1", PreparedName: "caller_owned", SkipPrepare: true}}

	plans, evicted := c.plan(connA, stmts)
	if len(evicted) != 0 {
		t.Fatalf("expected no evictions, got %v", evicted)
	}
	if plans[0].managed {
		t.Fatalf("a statement with a manual PreparedName must not be cache-managed, got %+v", plans[0])
	}
	if plans[0].name != "caller_owned" || !plans[0].skip {
		t.Fatalf("expected the caller's own name/skip to pass through unchanged, got %+v", plans[0])
	}

	// commit must be a no-op for unmanaged statements.
	c.commit(connA, stmts, plans, []error{nil})
	plans2, _ := c.plan(connA, []Statement{{SQL: "SELECT 1"}})
	if !plans2[0].isMiss {
		t.Fatalf("a manually-named statement must not pollute the cache for the same SQL text used without PreparedName, got %+v", plans2[0])
	}
}
