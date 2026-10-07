package multistmt

import (
	"context"
	"database/sql"
	"strconv"
)

// BuildSetOnlySQL renders the same SET sequence Execute would send ahead of
// each statement's PREPARE/EXECUTE — including the leading
// "SET @_multistmt_statement_num=0;" reset and, for a statement with Args,
// the same literal-encoded argument assignments merged into its marker SET
// (see build's doc comment for the full format and why marker+args share one
// dispatch) — but omits PREPARE, EXECUTE, and DEALLOCATE entirely.
//
// This exists to isolate the real cost of a workload's SET traffic: pointing
// it at the same Batch a caller would otherwise Execute reproduces the exact
// SET volume and argument shapes production traffic generates, without
// touching the underlying tables or running any real query, so it can be
// sent at matching throughput as an A/B baseline against the full batch.
func (b *Batch) BuildSetOnlySQL() (string, error) {
	var body []byte
	body = append(body, "SET "...)
	body = append(body, statementNumVar...)
	body = append(body, "=0;"...)

	for i, s := range b.stmts {
		name := s.PreparedName
		if name == "" {
			name = nextGeneratedName()
		}

		body = append(body, "SET "...)
		body = append(body, statementNumVar...)
		body = append(body, '=')
		body = append(body, strconv.Itoa(i+1)...)

		for k, a := range s.Args {
			lit, err := sqlValueLiteral(a)
			if err != nil {
				return "", err
			}
			body = append(body, ", @_multistmt_"...)
			body = append(body, name...)
			body = append(body, '_')
			body = append(body, strconv.Itoa(k)...)
			body = append(body, '=')
			body = append(body, lit...)
		}
		body = append(body, ';')
	}

	return string(body), nil
}

// ExecuteSetOnly sends BuildSetOnlySQL's output to conn as a single round
// trip — the same conn requirement (a single, stable *sql.Conn with
// multiStatements=true on its DSN) as Execute, but with no PREPARE, EXECUTE,
// or real query reaching the server.
func (b *Batch) ExecuteSetOnly(ctx context.Context, conn *sql.Conn) error {
	sqlText, err := b.BuildSetOnlySQL()
	if err != nil {
		return err
	}
	_, err = conn.ExecContext(ctx, sqlText)
	return err
}
