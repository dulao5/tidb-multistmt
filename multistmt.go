// Package multistmt lets an application queue several SQL statements and
// send them to TiDB (or MySQL) as a single multi-statement round trip,
// while still getting per-statement success/failure/skip information back —
// something the standard database/sql + go-sql-driver/mysql API cannot do on
// its own, because driver.Rows.NextResultSet() silently collapses every
// OK-only (non-SELECT) response in the stream.
//
// The trick: a "SET @_statement_num = N" marker is interleaved before every
// queued statement. @_statement_num is a user-defined session variable, so
// it survives a ROLLBACK — if the batch aborts partway through (TiDB stops
// executing the rest of the batch on the first error, like MySQL), a single
// follow-up "SELECT @_statement_num" on the same connection says exactly
// which statement was being attempted when the failure happened. Statements
// before that one succeeded; that one failed; everything after it was never
// executed at all.
//
// This only works at the application layer: it cannot recover the
// driver-hidden affected-rows count for a non-SELECT statement inside the
// batch (go-sql-driver never exposes it there), only whether that statement
// succeeded, failed, or was skipped.
package multistmt

import (
	"errors"
	"fmt"
)

// ErrSkipped is the error delivered to every statement queued after the one
// that failed in a batch — the TiDB/MySQL equivalent of PostgreSQL pipeline
// mode's PGRES_PIPELINE_ABORTED. These statements were never sent to the
// server at all.
var ErrSkipped = errors.New("multistmt: statement skipped because an earlier statement in the batch failed")

// Statement is one queued unit of work.
type Statement struct {
	// SQL is the statement text, with "?" placeholders for Args — exactly
	// like database/sql. It is always executed as a server-side prepared
	// statement (PREPARE ... FROM / EXECUTE ... USING), even for a single
	// use, matching PostgreSQL pipeline mode's PQsendQueryParams() model.
	SQL string

	// Args are bound, in order, to SQL's "?" placeholders.
	Args []any

	// HasResultSet must be true iff SQL is a row-returning statement
	// (SELECT, SHOW, ...). The caller must set this correctly: it is what
	// lets Execute tell this statement's response apart from the OK-only
	// responses of surrounding non-SELECT statements, which are otherwise
	// invisible to the driver. Getting it wrong desyncs every later
	// statement in the same batch.
	HasResultSet bool

	// Callback, if non-nil, is invoked exactly once by Execute, synchronously,
	// in queue order. For a HasResultSet statement, Callback runs while its
	// *sql.Rows is still positioned at that result — the callback must fully
	// consume it (e.g. via Scan in a loop) before returning, since Execute
	// advances to the next result as soon as Callback returns.
	Callback func(*StatementResult)

	// PreparedName, if set, is used as a stable SQL-level PREPARE name
	// instead of a freshly generated one, and this statement's prepared
	// statement is NOT deallocated at the end of the batch — letting the
	// caller reuse it across multiple Batch.Execute calls on the same
	// long-lived connection. This is the "orthogonal cache layer": Execute
	// itself does not track or dedupe across calls, so the caller must track,
	// per connection, whether a given PreparedName has already been PREPAREd
	// and set SkipPrepare accordingly.
	PreparedName string

	// SkipPrepare, when true (only meaningful together with PreparedName),
	// tells Execute to skip emitting PREPARE for this statement and go
	// straight to SET+EXECUTE, assuming PreparedName is already prepared on
	// this connection from an earlier Batch.Execute call.
	SkipPrepare bool
}

// StatementResult is what Callback receives, and what BatchError reports
// position for.
type StatementResult struct {
	// Index is this statement's 0-based position in the batch (the order it
	// was added via Batch.Add).
	Index int

	// SQL is this statement's SQL text, copied from Statement.SQL, included
	// for convenience in logging/error messages.
	SQL string

	HasResultSet bool

	// Rows is non-nil only when HasResultSet is true and Err is nil. The
	// caller owns reading it (Next/Scan) but must not call Close — Execute
	// manages the underlying multi-result-set stream's lifecycle.
	Rows RowsScanner

	// Err is nil on success, ErrSkipped if this statement was never
	// executed because an earlier one in the batch failed, or the actual
	// driver/server error if this was the statement that failed.
	Err error
}

// RowsScanner is the subset of *sql.Rows a Callback needs. It exists so
// Execute isn't forced to construct a *sql.Rows for statements it can also
// deliver via test doubles.
type RowsScanner interface {
	Next() bool
	Scan(dest ...any) error
	Columns() ([]string, error)
	Err() error
}

// BatchError is the aggregate error Execute returns when any statement in
// the batch failed or was skipped. It wraps the first real failure (not a
// skip) so errors.As/errors.Is keep working against the underlying driver
// error.
type BatchError struct {
	// Index is the 0-based position of the statement that actually failed.
	// Verified against a real TiDB: each statement (including a malformed
	// one) is parsed and executed incrementally, so a syntax error in
	// statement N does not prevent statements before N from running — Index
	// correctly points at N in that case too, not an earlier one. -1 is only
	// used as a last-resort fallback for failures that happen before the
	// batch's own position-recovery marker could be set at all (e.g. the
	// connection drops before any bytes of the batch are acknowledged).
	Index int

	// SQL is the failing statement's text ("" when Index is -1).
	SQL string

	// Err is the underlying error returned by the server/driver for the
	// failing statement.
	Err error
}

func (e *BatchError) Error() string {
	if e.Index < 0 {
		return fmt.Sprintf("multistmt: batch failed before any statement executed: %v", e.Err)
	}
	return fmt.Sprintf("multistmt: statement #%d (%s) failed: %v", e.Index, e.SQL, e.Err)
}

func (e *BatchError) Unwrap() error { return e.Err }

// Batch is an ordered queue of Statements to send as one multi-statement
// round trip. The zero value is not usable; create one with New.
type Batch struct {
	stmts []Statement
}

// New creates an empty Batch.
func New() *Batch {
	return &Batch{}
}

// Add queues sqlText (with its Args) for execution, returning the Batch for
// chaining. hasResultSet and cb are as documented on Statement.
func (b *Batch) Add(sqlText string, args []any, hasResultSet bool, cb func(*StatementResult)) *Batch {
	b.stmts = append(b.stmts, Statement{
		SQL:          sqlText,
		Args:         args,
		HasResultSet: hasResultSet,
		Callback:     cb,
	})
	return b
}

// AddStatement queues a fully constructed Statement (e.g. one using
// PreparedName/SkipPrepare), returning the Batch for chaining.
func (b *Batch) AddStatement(s Statement) *Batch {
	b.stmts = append(b.stmts, s)
	return b
}

// Len returns the number of queued statements.
func (b *Batch) Len() int { return len(b.stmts) }

// Statements returns a copy of the queued statements, in order. Execute
// itself has no use for this; it exists for callers that build a Batch in
// one place and want to inspect what was queued (e.g. in tests) without
// reaching into unexported state.
func (b *Batch) Statements() []Statement {
	return append([]Statement(nil), b.stmts...)
}
