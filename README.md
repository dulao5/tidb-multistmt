# tidb-multistmt

Application-layer, Go `database/sql`-only library that gives
[PostgreSQL pipeline-mode](https://www.postgresql.org/docs/current/libpq-pipeline-mode.html)-style
per-statement result handling on top of TiDB/MySQL's multi-statement protocol —
without touching the server, the protocol, or `go-sql-driver/mysql` itself.

## Why

TiDB (and MySQL) support sending several statements as one `COM_QUERY` round
trip (`?multiStatements=true` in the DSN). If one statement in the batch
fails, the server stops executing the rest of the batch, just like
PostgreSQL's pipeline mode does when an error occurs.

The problem is entirely on the client side: Go's `database/sql` +
`go-sql-driver/mysql` abstracts a multi-statement response stream as a
sequence of `Rows`, but `Rows.NextResultSet()` *silently skips every
OK-only (non-`SELECT`) response* — there is no way, using the standard API,
to tell how many non-`SELECT` statements in the batch actually ran before an
error, or exactly which one failed. (See the package doc comment and this
repo's companion write-up for the exact driver code that does this.)

PostgreSQL's `libpq` doesn't have this problem: `PQgetResult()` returns one
`PGresult` per queued statement, command or query alike, with an explicit
`PGRES_PIPELINE_ABORTED` status for every statement skipped after a failure.
This library reproduces that experience over TiDB/MySQL, entirely in Go,
with no server or protocol changes.

## How

Every statement you `Add` gets wrapped as a server-side prepared statement
(`PREPARE ... FROM` / `EXECUTE ... USING`), and immediately before it, the
library injects `SET @_multistmt_statement_num = N`. That's a user-defined
session variable — it survives `ROLLBACK` — so if the batch aborts partway
through, a single follow-up `SELECT @_multistmt_statement_num` on the same
connection says exactly which statement was being attempted when the
failure happened. Everything before it succeeded; that one failed;
everything after it was never sent to the server at all.

This only recovers *position*, not the hidden affected-rows count for a
non-`SELECT` statement — that data genuinely isn't reachable through
`database/sql`'s API in a multi-result stream. If you need per-statement
affected-rows, you need a lower-level MySQL client (e.g. `mysql_next_result()`
via cgo, or PHP's `mysqli_next_result()`), which this library's issue tracker
links to for anyone who wants to build it.

## Usage

```go
b := multistmt.New()

b.Add("INSERT INTO accounts (id, balance) VALUES (?, ?)", []any{1, 100}, false,
    func(r *multistmt.StatementResult) {
        if r.Err != nil {
            log.Printf("insert failed: %v", r.Err)
        }
    })

b.Add("SELECT balance FROM accounts WHERE id = ?", []any{1}, true,
    func(r *multistmt.StatementResult) {
        if r.Err != nil {
            return
        }
        for r.Rows.Next() {
            var balance int
            r.Rows.Scan(&balance)
            fmt.Println("balance:", balance)
        }
    })

conn, _ := db.Conn(ctx) // a single, stable *sql.Conn — not *sql.DB
err := b.Execute(ctx, conn)

var batchErr *multistmt.BatchError
if errors.As(err, &batchErr) {
    fmt.Printf("statement #%d failed: %v\n", batchErr.Index, batchErr.Err)
}
```

- `HasResultSet` must be `true` iff the statement is row-returning
  (`SELECT`/`SHOW`/...). The library needs this to tell your statement's
  response apart from the invisible OK packets of surrounding non-`SELECT`
  statements — get it wrong and every later statement in the batch desyncs.
- `Callback` runs synchronously, in queue order, during `Execute`. For a
  `HasResultSet` statement it receives the live result positioned at that
  statement's rows — consume it (`Next`/`Scan`) before returning, since
  `Execute` advances past it as soon as `Callback` returns.
- On success every statement's callback gets `Err: nil`.
- On a mid-batch failure: statements before the failure get `Err: nil`, the
  failing one gets the real error, everything after it gets
  [`ErrSkipped`] — `errors.Is(r.Err, multistmt.ErrSkipped)`.
- `Execute`'s own return value is a `*multistmt.BatchError` (nil on success)
  carrying the same failing statement's index, SQL text, and underlying
  error, for callers who'd rather check one place than rely on every
  individual callback — e.g. to decide whether to send a `ROLLBACK` you
  queued yourself.
- `BEGIN`/`COMMIT`/`ROLLBACK` are not special-cased — queue them as regular
  statements (`HasResultSet: false`) if you want transactional semantics;
  the library stays agnostic to what the statements actually do.

### Prepared-statement caching

By default every `Execute` call generates a fresh `PREPARE` name for each
statement and `DEALLOCATE PREPARE`s it at the end of the same batch — no
session state leaks across calls, but nothing is reused either: a batch
re-executed a thousand times re-compiles every statement a thousand times.

**Best practice: pass `WithPreparedCache` to `Execute`, backed by a shared
`*PreparedCache`, not manual `PreparedName` bookkeeping.** `(*sql.Stmt)` from
plain `db.Prepare()` already solves prepared-statement reuse for free for *a
single, repeated statement* — it transparently tracks, per physical
connection, whether it's already been prepared there, so you never need to
think about it. That's the right tool when you're always running the same
one statement. It does **not** help here, because a multistmt batch isn't one
statement — it's N different statements sent as one hand-built
multi-statement string (`PREPARE`/`SET`/`EXECUTE`), which never goes through
`PrepareContext` at all. `PreparedCache` is what gives a *batch of distinct
statements* the same transparent reuse `*sql.Stmt` gives a single one:

```go
// defaults: 32 stmts/conn, 256 conns tracked; share one PreparedCache across your whole app
cache := multistmt.NewPreparedCache(0, 0)

// ... build b as usual with Add/AddStatement, no PreparedName needed ...

conn, _ := db.Conn(ctx) // a single, stable *sql.Conn for this call
err := b.Execute(ctx, conn, multistmt.WithPreparedCache(cache))
```

That's the whole API. `PreparedCache` figures out, per statement, whether
it's already `PREPARE`d on `conn`'s *physical* connection and skips
re-preparing it if so — including when `conn` is a brand new `*sql.Conn`
wrapper handed back by the pool around a physical connection `PreparedCache`
has seen before. (This is the part naive per-connection caching gets wrong:
`database/sql` gives you a fresh `*sql.Conn` Go object on every
`db.Conn(ctx)` checkout even when the pool reuses the same underlying TCP
connection/TiDB session underneath, so keying a cache by the `*sql.Conn`
pointer itself would miss every reuse opportunity across checkouts.
`PreparedCache` keys by the physical connection instead, via
`(*sql.Conn).Raw`.) Past `maxStmtsPerConn`, the least-recently-used statement
on that connection is `DEALLOCATE`d (in the same round trip that needed the
room) to make space — you don't have to think about unbounded growth either.

`PreparedCache` is safe to share across goroutines/connections — create one
per `*sql.DB` (or per process) and pass it via `WithPreparedCache` to every
`Execute` call.

If you still want fully manual control (e.g. a short-lived connection you
know will never see a repeat, or a name you need to coordinate outside this
library), `Statement.PreparedName`/`SkipPrepare` are still there, and
`PreparedCache` leaves any statement that sets `PreparedName` itself alone
entirely:

```go
b.AddStatement(multistmt.Statement{
    SQL:          "INSERT INTO accounts (id, balance) VALUES (?, ?)",
    Args:         []any{id, balance},
    PreparedName: "ins_account",
    SkipPrepare:  alreadyPreparedOnThisConn, // you own this bookkeeping
})
```

When `PreparedName` is set, `Execute` never deallocates it — you own its
lifecycle for the life of the connection, whether or not `WithPreparedCache`
is also passed.

(Single-statement, non-batched reuse — "just run this one query over and
over on a pooled connection" — is already solved by `database/sql` itself via
`db.Prepare()`/`*sql.Stmt`; that's the right tool for that job and this
library doesn't try to replace it. `PreparedCache` only exists for the
batch-of-distinct-statements case `*sql.Stmt` can't cover.)

### `WHERE id IN (?)` with a variable-length list

`Statement.Args` binds one Go value per `?`, so a plain `?` can't carry a
variable-length list by itself. `ExpandIn` rewrites the SQL text and flattens
the args before you call `Add`:

```go
sqlText, args, err := multistmt.ExpandIn(
    "SELECT balance FROM accounts WHERE id IN (?)",
    []any{ids}, // ids is a []int
)
if err != nil {
    log.Fatal(err)
}
b.Add(sqlText, args, true, func(r *multistmt.StatementResult) { ... })
```

With `ids = []int{1, 2, 3}`, this turns into
`SELECT balance FROM accounts WHERE id IN (?,?,?)` plus three flattened `int`
args — exactly the placeholder count `EXECUTE ... USING` needs. Non-slice
args in the same statement pass through untouched, and a `[]byte` arg is
never expanded (it's a single blob value, matching `database/sql`'s own
convention). An empty slice is rejected (`ErrEmptyInArgs`) rather than
silently becoming `IN (NULL)`, since that would change the query's meaning,
not just its placeholder count.

One thing to know before combining this with `PreparedCache`: the cache key
is the final SQL text, and `ExpandIn` bakes the list's length into that text
(`IN (?,?,?)` vs `IN (?,?)` are different statements to `PREPARE`). That's
correct — a placeholder count has to match the list it binds — but it means
a call site whose list length varies a lot on every call gets little benefit
from `PreparedCache`, since every new length is a cache miss. If that
matters for your workload, pad the list up to a fixed set of bucket sizes
(e.g. always call with length 1, 4, 16, or 64, repeating the last element to
fill) before calling `ExpandIn`, so the same few SQL texts repeat and
actually get cached.

### Bulk `INSERT ... VALUES (?), (?), (?)`

`ExpandValues` is `ExpandIn`'s counterpart for the opposite shape: instead of
one value becoming N values, one *row* becomes N rows. Write a single-row
template and pass your rows as `[][]any`:

```go
rows := [][]any{
    {1, 100},
    {2, 200},
    {3, 300},
}
sqlText, args, err := multistmt.ExpandValues(
    "INSERT INTO accounts (id, balance) VALUES (?, ?)", // one-row template
    rows,
)
if err != nil {
    log.Fatal(err)
}
b.Add(sqlText, args, false, nil)
```

This turns into `INSERT INTO accounts (id, balance) VALUES (?,?),(?,?),(?,?)`
plus the six args flattened in row order. `ExpandValues` requires the
template to contain exactly one parenthesized, placeholder-only group — the
`(id, balance)` column list isn't one (it names columns, not `?`s), so it's
left alone; only the `(?, ?)` after `VALUES` qualifies. Every row must have
the same length as that group's placeholder count, or `ExpandValues` errors
naming the offending row index. This only covers "one column set, one VALUES
shape" — per-row `ON DUPLICATE KEY UPDATE` or varying column sets per row are
not something `ExpandValues` tries to generate; write that part of the SQL
text yourself around the template, same as with any other statement.

`ExpandValues` never formats a value into SQL itself — it only rewrites
placeholder text and reorders/flattens the Go values you gave it. Every
element of every row is escaped exactly the way a plain `Statement.Args`
element always is (see `sqlValueLiteral` in `build.go`), so there's no
separate escaping path for bulk inserts to audit. That shared path now also
accepts any type implementing `driver.Valuer` (`sql.NullString`,
`sql.NullInt64`, a custom column type, ...), formatting whatever
`driver.Value` it returns the same way as a plain value of that type.

**On that shared escaping path's own limits** (not new to `ExpandValues`,
but worth stating explicitly since bulk inserts are the usage this library
expects to carry the most untrusted-ish data): this library has no wire-level
parameter binding to fall back on — every arg is embedded as literal SQL text
via `SET @v=<literal>` (see the package doc comment for why: a batch is one
hand-built multi-statement string, never routed through
`(*sql.DB).PrepareContext`). `sqlStringLiteral`'s escaping matches TiDB/MySQL's
default string-literal syntax, and is only safe under that default: a session
running with `sql_mode=...,NO_BACKSLASH_ESCAPES` (backslash stops being
special) or a connection using a legacy multi-byte charset where a lead byte
can absorb the following escape character (the classic "GBK injection" class
of bug — TiDB's `utf8`/`utf8mb4`/`latin1`/`binary` charsets don't have this
problem) breaks this escaping's safety guarantee. Don't pass untrusted string
data through `Statement.Args`/`ExpandIn`/`ExpandValues` under either of those
configurations.

The same cache-key caveat as `ExpandIn` applies here, with a different
mitigation: `PreparedCache` keys on the final SQL text, and `ExpandValues`
bakes the row count into it, so a call site whose batch size varies a lot
gets little benefit from the cache. Unlike `ExpandIn`'s list case, you can't
safely "pad" a batch of rows the way you can pad a value list (padding would
insert extra, possibly conflicting, rows) — the natural fix here is fixed-size
chunking instead: always call `ExpandValues` with exactly N rows per batch,
letting the final, possibly-shorter chunk miss the cache once.

### Best practice: prepared-cache + error handling + one connection

The three pieces above (one stable `*sql.Conn`, `PreparedCache`, and
`errors.As`-based error handling) are meant to be used together, on a
connection that runs many transactions over its lifetime — not just in a
single `Execute` call. This is the pattern a production caller should copy:

```go
// One PreparedCache per *sql.DB (or per process) — it's keyed by physical
// connection, so it's shared safely across every conn this pool hands out.
cache := multistmt.NewPreparedCache(0, 0)

conn, err := db.Conn(ctx) // a single, stable *sql.Conn — not *sql.DB
if err != nil {
    log.Fatal(err)
}
defer conn.Close()

for {
    b := multistmt.New()
    b.Add("begin", nil, false, nil)
    b.Add("INSERT INTO accounts (id, balance) VALUES (?, ?)", []any{id, balance}, false, nil)
    b.Add("UPDATE accounts SET balance = balance - ? WHERE id = ?", []any{amount, from}, false, nil)
    b.Add("commit", nil, false, nil)

    err := b.Execute(ctx, conn, multistmt.WithPreparedCache(cache))
    if err == nil {
        continue // conn is clean (commit ran); reuse it for the next transaction
    }

    var batchErr *multistmt.BatchError
    if errors.As(err, &batchErr) {
        log.Printf("statement #%d (%s) failed: %v", batchErr.Index, batchErr.SQL, batchErr.Err)
    } else {
        log.Printf("batch failed before any statement executed: %v", err)
    }

    // The batch's own trailing "commit" never ran, but "begin" did — conn is
    // sitting on an open, uncommitted transaction. BEGIN/COMMIT/ROLLBACK
    // aren't special-cased by this library (see above), so rolling back is
    // the caller's job, same as it would be with any other driver call that
    // leaves a transaction open.
    if _, rbErr := conn.ExecContext(ctx, "ROLLBACK"); rbErr != nil {
        log.Printf("rollback failed, conn is no longer usable: %v", rbErr)
        return
    }
}
```

Across every iteration of this loop, PREPARE only happens once per distinct
statement *text* per physical connection — `PreparedCache` recognizes the
same `INSERT`/`UPDATE` text on the next iteration and reuses what the first
iteration already prepared, even though `b` itself is a brand new `*Batch`
every time. A failed iteration's `ROLLBACK` does not evict anything from the
cache: the prepared statements themselves are still valid on the connection,
only the data they touched got rolled back.

## Status

Verified against a live TiDB (v8.5.8) covering: all-non-SELECT batches, mixed
batches with real row delivery, a mid-batch runtime failure (duplicate key),
a failure on the very first statement, a SQL syntax error on a later
statement (confirmed: TiDB parses/executes each statement incrementally, so
earlier well-formed statements still ran and are correctly reported as
succeeded), prepared-statement reuse across separate `Execute` calls via
manual `PreparedName`, and `PreparedCache`/`WithPreparedCache`:
same-physical-connection identity across `database/sql` pool checkouts
(`MaxOpenConns(1)`, verified the
identity is stable across `Close`+`db.Conn` even though the `*sql.Conn` Go
object changes), a cache hit on a second checkout correctly reusing the first
checkout's server-side `PREPARE` (which would fail loudly with "Unknown
prepared statement" from the real server if the identity tracking were
wrong), and LRU eviction really issuing `DEALLOCATE PREPARE` on the server
(confirmed by `EXECUTE`-ing the evicted name directly afterward and getting
an error), `ExpandIn` producing a SELECT whose `IN (?)` list actually
returns the expected multiple rows, and `ExpandValues` bulk-inserting several
rows — including rows containing quotes, backslashes, and a
SQL-injection-shaped string (`'; DROP TABLE ...; --`) — in one round trip and
reading every one back correctly (the table surviving the round trip is
itself part of what's being checked). See
`build_test.go`/`cache_test.go`/`inexpand_test.go`/`values_test.go` (pure
unit tests) and `integration_test.go`/`cache_integration_test.go` (gated
behind `MULTISTMT_TEST_DSN`, not required for `go test ./...`).

Not yet covered: contexts with `QueryRowContext`-style single-row
conveniences, and any benchmark of the actual throughput/latency trade-off
against one-statement-per-round-trip — this library is about *correctness of
per-statement result handling*, not a performance claim.
