# tidb-multistmt

API reference for [github.com/dulao5/tidb-multistmt](https://github.com/dulao5/tidb-multistmt),
generated from the package's own Go doc comments. For the runnable
best-practice example this page cross-references, see the
[repository README](https://github.com/dulao5/tidb-multistmt#readme).

## Concept

Application-layer, Go `database/sql`-only library that gives
[PostgreSQL pipeline-mode](https://www.postgresql.org/docs/current/libpq-pipeline-mode.html)-style
per-statement result handling on top of TiDB/MySQL's multi-statement protocol —
without touching the server, the protocol, or `go-sql-driver/mysql` itself.

TiDB (and MySQL) support sending several statements as one `COM_QUERY` round
trip (`?multiStatements=true` in the DSN). If one statement in the batch
fails, the server stops executing the rest of the batch, just like
PostgreSQL's pipeline mode does when an error occurs.

The problem is entirely on the client side: Go's `database/sql` +
`go-sql-driver/mysql` abstracts a multi-statement response stream as a
sequence of `Rows`, but `Rows.NextResultSet()` *silently skips every
OK-only (non-`SELECT`) response* — there is no way, using the standard API,
to tell how many non-`SELECT` statements in the batch actually ran before an
error, or exactly which one failed.

The trick, implemented by `Batch.Execute`: every statement is wrapped as a
server-side prepared statement (`PREPARE ... FROM` / `EXECUTE ... USING`),
and immediately before it, a `SET @_multistmt_statement_num = N` marker is
injected. That session variable survives `ROLLBACK`, so if the batch aborts
partway through, a follow-up `SELECT @_multistmt_statement_num` on the same
connection says exactly which statement was being attempted when the failure
happened — everything before it succeeded, that one failed, everything after
it was never sent to the server at all. This recovers *position*, not a
failed non-`SELECT` statement's hidden affected-rows count, which genuinely
isn't reachable through `database/sql`'s API in a multi-result stream.

## Best practice: prepared-cache + error handling + one connection

Production code should combine three pieces, all documented below:

1. **One stable `*sql.Conn`** (not `*sql.DB`) — `Batch.Execute`'s
   position-recovery marker is a session variable, so `PREPARE`/`EXECUTE`
   must run on the same session they were issued on.
2. **A shared `PreparedCache`**, passed via `WithPreparedCache`, so repeated
   `Execute` calls on the same physical connection reuse each statement's
   server-side `PREPARE` instead of recompiling it every time.
3. **`errors.As` against `*BatchError`** on every `Execute` failure, to log
   exactly which statement (index + SQL text) failed, followed by an
   explicit `ROLLBACK` — `Execute` doesn't special-case `BEGIN`/`COMMIT`, so
   if your batch's own trailing `commit` never ran, the connection is left
   sitting on an open transaction until you roll it back yourself.

See the [README's worked example](https://github.com/dulao5/tidb-multistmt#best-practice-prepared-cache--error-handling--one-connection)
for the full runnable loop. The API reference below documents each piece
individually.

---

