package multistmt

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// Execute sends the whole batch to conn as a single multi-statement round
// trip, invoking each queued Statement's Callback exactly once, in order,
// and returns a non-nil *BatchError if any statement failed (wrapping the
// underlying driver/server error; see BatchError).
//
// conn must be a single, stable connection (*sql.Conn, not *sql.DB) for the
// whole call: the position-recovery marker is a session variable, and
// PREPARE/EXECUTE must run on the same session they were issued on.
func (b *Batch) Execute(ctx context.Context, conn *sql.Conn) error {
	if len(b.stmts) == 0 {
		return nil
	}

	built, err := b.build()
	if err != nil {
		return err
	}

	rows, queryErr := conn.QueryContext(ctx, built.sql)
	if queryErr != nil {
		return b.recoverAndReport(ctx, conn, queryErr)
	}
	defer rows.Close()

	hasResultIdx := make([]int, 0, len(b.stmts))
	for i, s := range b.stmts {
		if s.HasResultSet {
			hasResultIdx = append(hasResultIdx, i)
		}
	}

	nextStmtIdx := 0 // first statement index not yet confirmed successful
	hrPos := 0       // position into hasResultIdx
	var execErr error

	landed := true // conn.QueryContext already positioned `rows` at the first result (or EOF, if the whole batch was non-SELECT)
	for landed {
		cols, _ := rows.Columns()
		if len(cols) == 0 {
			// No active result set here: either genuine end of stream, or
			// (shouldn't happen) a malformed empty result. Either way there's
			// nothing to deliver via this landing.
			break
		}
		if hrPos >= len(hasResultIdx) {
			execErr = fmt.Errorf("multistmt: got a row-returning result but every declared HasResultSet statement was already delivered (a statement's HasResultSet is probably mis-declared as false)")
			break
		}

		idx := hasResultIdx[hrPos]
		b.fireSuccessRange(nextStmtIdx, idx)
		b.fireCallback(idx, rows, nil)
		nextStmtIdx = idx + 1
		hrPos++

		landed = rows.NextResultSet()
	}

	if execErr == nil {
		execErr = rows.Err()
	}

	if execErr != nil {
		return b.recoverAndReportFrom(ctx, conn, nextStmtIdx, execErr)
	}

	// Clean completion: everything from nextStmtIdx to the end succeeded
	// (including any HasResultSet statements after the last one we actually
	// landed on would be a contradiction — hrPos must equal len(hasResultIdx)
	// here, so the remaining tail, if any, is all non-HasResultSet).
	b.fireSuccessRange(nextStmtIdx, len(b.stmts))
	return nil
}

// fireSuccessRange invokes Callback with Err: nil for statements [from, to).
// Only used for statements that have no result set of their own to deliver —
// their success is inferred from execution having reached a later point in
// the batch (or the clean end of it) without error.
func (b *Batch) fireSuccessRange(from, to int) {
	for i := from; i < to; i++ {
		b.fireCallback(i, nil, nil)
	}
}

func (b *Batch) fireCallback(idx int, rows RowsScanner, err error) {
	s := &b.stmts[idx]
	if s.Callback == nil {
		return
	}
	s.Callback(&StatementResult{
		Index:        idx,
		SQL:          s.SQL,
		HasResultSet: s.HasResultSet,
		Rows:         rows,
		Err:          err,
	})
}

// recoverAndReport handles a failure that happened before Execute could even
// confirm reaching the first result — most commonly statement 0 itself
// failing immediately, or the connection dying outright. It still attempts
// the @_multistmt_statement_num recovery query in all cases: each statement
// is parsed and executed incrementally, not as one upfront whole-batch
// parse, so the marker is reliable even when the very first statement is the
// one that failed.
func (b *Batch) recoverAndReport(ctx context.Context, conn *sql.Conn, queryErr error) error {
	return b.recoverAndReportFrom(ctx, conn, 0, queryErr)
}

// recoverAndReportFrom performs the position-recovery query and delivers the
// success/failure/skip callbacks for the remainder of the batch.
// knownGoodUpTo is the index already confirmed successful by Execute's own
// bookkeeping (every HasResultSet statement up to here was already
// delivered); the recovery query is still needed to find out, among the
// statements from knownGoodUpTo onward, exactly which one failed.
func (b *Batch) recoverAndReportFrom(ctx context.Context, conn *sql.Conn, knownGoodUpTo int, execErr error) error {
	n, recErr := readStatementNum(ctx, conn)

	failedIdx := -1
	switch {
	case recErr != nil:
		// Can't even run the recovery query (e.g. connection is dead) — we
		// only know statements before knownGoodUpTo were fine; treat
		// everything from knownGoodUpTo onward as indeterminate/failed at
		// knownGoodUpTo.
		failedIdx = knownGoodUpTo
	case n <= 0:
		// Nothing in this batch ever executed (e.g. a parse error affecting
		// the whole multi-statement text).
		failedIdx = -1
	default:
		// n is 1-based (SET @_multistmt_statement_num=i+1 runs immediately
		// before statement i), so statement n-1 is the one that was being
		// attempted when the failure happened.
		failedIdx = n - 1
		if failedIdx < knownGoodUpTo {
			// Shouldn't happen, but don't let a stale/inconsistent read move
			// the failure point backwards past what Execute already
			// confirmed succeeded.
			failedIdx = knownGoodUpTo
		}
	}

	if failedIdx < 0 {
		for i := range b.stmts {
			b.fireCallback(i, nil, ErrSkipped)
		}
		return &BatchError{Index: -1, Err: execErr}
	}

	b.fireSuccessRange(knownGoodUpTo, failedIdx)
	b.fireCallback(failedIdx, nil, execErr)
	for i := failedIdx + 1; i < len(b.stmts); i++ {
		b.fireCallback(i, nil, ErrSkipped)
	}

	return &BatchError{Index: failedIdx, SQL: b.stmts[failedIdx].SQL, Err: execErr}
}

func readStatementNum(ctx context.Context, conn *sql.Conn) (int, error) {
	var n sql.NullInt64
	row := conn.QueryRowContext(ctx, "SELECT "+statementNumVar)
	if err := row.Scan(&n); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}
	if !n.Valid {
		return 0, nil
	}
	return int(n.Int64), nil
}
