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

### Prepared-statement caching (optional, orthogonal)

By default every `Execute` call generates a fresh `PREPARE` name for each
statement and `DEALLOCATE PREPARE`s it at the end of the same batch — no
session state leaks across calls. If you want to reuse a prepared statement
across multiple `Execute` calls on the same long-lived connection (to save
repeated re-compilation), set a stable `Statement.PreparedName` and track
yourself, per connection, whether it's already been prepared:

```go
b.AddStatement(multistmt.Statement{
    SQL:          "INSERT INTO accounts (id, balance) VALUES (?, ?)",
    Args:         []any{id, balance},
    PreparedName: "ins_account",
    SkipPrepare:  alreadyPreparedOnThisConn, // you own this bookkeeping
})
```

When `PreparedName` is set, `Execute` never deallocates it — you own its
lifecycle for the life of the connection.

## Status

Verified against a live TiDB (v8.5.8) covering: all-non-SELECT batches, mixed
batches with real row delivery, a mid-batch runtime failure (duplicate key),
a failure on the very first statement, a SQL syntax error on a later
statement (confirmed: TiDB parses/executes each statement incrementally, so
earlier well-formed statements still ran and are correctly reported as
succeeded), and prepared-statement reuse across separate `Execute` calls. See
`build_test.go` (pure unit tests) and `integration_test.go` (gated behind
`MULTISTMT_TEST_DSN`, not required for `go test ./...`).

Not yet covered: array/slice-valued args (`IN (?)` expansion), contexts with
`QueryRowContext`-style single-row conveniences, and any benchmark of the
actual throughput/latency trade-off against one-statement-per-round-trip —
this library is about *correctness of per-statement result handling*, not a
performance claim.
