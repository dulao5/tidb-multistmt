package multistmt

import (
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// statementNumVar is the session variable name used for the position-recovery
// marker. It is reset to 0 at the start of every batch so a post-failure read
// unambiguously distinguishes "nothing in this batch executed yet" from a
// stale value left over by an earlier, unrelated batch on the same
// connection.
const statementNumVar = "@_multistmt_statement_num"

var preparedNameSeq atomic.Uint64

// nextGeneratedName returns a process-wide-unique SQL-level PREPARE name for
// a statement that didn't set Statement.PreparedName.
func nextGeneratedName() string {
	return fmt.Sprintf("_multistmt_ps_%d", preparedNameSeq.Add(1))
}

// buildResult is everything build() computed that execute() needs
// afterwards.
type buildResult struct {
	sql string
	// dealloc lists the PREPARE names this batch generated and must
	// DEALLOCATE PREPARE at the end (PreparedName statements are excluded:
	// the caller owns their lifecycle across calls).
	dealloc []string
}

// build renders the whole batch into one multi-statement SQL string:
//
//	SET @_multistmt_statement_num=0;
//	PREPARE p1 FROM '...stmt 0...';
//	SET @_multistmt_statement_num=1, @_multistmt_ps_1_0=<literal>, ...;
//	EXECUTE p1 USING @_multistmt_ps_1_0, ...;
//	PREPARE p2 FROM '...stmt 1...';
//	SET @_multistmt_statement_num=2;
//	EXECUTE p2;
//	...
//	DEALLOCATE PREPARE p1;
//	DEALLOCATE PREPARE p2;
//
// Every generated PREPARE is deallocated at the end of the same batch unless
// the statement set PreparedName (the caller then owns its lifecycle). The
// SET @_multistmt_statement_num=N marker is placed immediately before each
// statement's own PREPARE, so that after a mid-batch failure, N (0-based:
// N-1) is exactly the index of the statement that was attempted when it
// failed. When the statement also has Args, their SET is folded into the
// very same SET statement as the marker (one dispatch instead of two) —
// safe because the marker's only hard constraint is "before this
// statement's PREPARE", and the args don't need PREPARE to have run yet
// either (their variable names are derived from the statement's prepared
// name string, not from PREPARE's own execution).
func (b *Batch) build(extraDealloc ...string) (*buildResult, error) {
	var body strings.Builder
	var dealloc []string

	body.WriteString("SET ")
	body.WriteString(statementNumVar)
	body.WriteString("=0;")

	for i, s := range b.stmts {
		name := s.PreparedName
		if name == "" {
			name = nextGeneratedName()
		}

		// The marker update must be the very first thing emitted for this
		// statement — strictly before its own PREPARE — so that even a
		// failure while TiDB compiles this statement's SQL text (e.g. a
		// syntax error inside the PREPARE ... FROM '...' argument) is
		// attributed to this statement, not the previous one. Any Args this
		// statement has are folded into the same SET (one dispatch instead
		// of a separate marker-SET-then-args-SET pair).
		body.WriteString("SET ")
		body.WriteString(statementNumVar)
		body.WriteString("=")
		body.WriteString(strconv.Itoa(i + 1))

		varNames := make([]string, len(s.Args))
		for k, a := range s.Args {
			lit, err := sqlValueLiteral(a)
			if err != nil {
				return nil, fmt.Errorf("multistmt: statement #%d arg %d: %w", i, k, err)
			}
			vn := fmt.Sprintf("@_multistmt_%s_%d", name, k)
			varNames[k] = vn
			body.WriteString(", ")
			body.WriteString(vn)
			body.WriteString("=")
			body.WriteString(lit)
		}
		body.WriteString(";")

		if !s.SkipPrepare {
			quoted, err := sqlStringLiteral(s.SQL)
			if err != nil {
				return nil, fmt.Errorf("multistmt: statement #%d: %w", i, err)
			}
			body.WriteString("PREPARE ")
			body.WriteString(name)
			body.WriteString(" FROM ")
			body.WriteString(quoted)
			body.WriteString(";")
		}

		body.WriteString("EXECUTE ")
		body.WriteString(name)
		if len(varNames) > 0 {
			body.WriteString(" USING ")
			body.WriteString(strings.Join(varNames, ", "))
		}
		body.WriteString(";")

		if s.PreparedName == "" {
			dealloc = append(dealloc, name)
		}
	}

	for _, name := range dealloc {
		body.WriteString("DEALLOCATE PREPARE ")
		body.WriteString(name)
		body.WriteString(";")
	}
	for _, name := range extraDealloc {
		body.WriteString("DEALLOCATE PREPARE ")
		body.WriteString(name)
		body.WriteString(";")
	}

	return &buildResult{sql: body.String(), dealloc: dealloc}, nil
}

// sqlStringLiteral quotes s as a single-quoted SQL string literal, for use
// as the argument to "PREPARE name FROM '...'" and every "SET @v='...'" this
// package emits for a string/[]byte-valued arg.
//
// Its escaping (backslash doubled, then single-quote backslash-escaped)
// matches TiDB/MySQL's own default string-literal syntax — the one in
// effect unless the session's sql_mode includes NO_BACKSLASH_ESCAPES, in
// which case backslash stops being special, and this function's output is
// no longer a safe literal. This package's whole binding mechanism works by
// embedding values as literal SQL text (there is no lower-level wire
// parameter binding to fall back on — see the package doc comment), so a
// caller running with NO_BACKSLASH_ESCAPES must not pass untrusted string
// data through Statement.Args/ExpandIn/ExpandValues. The same is true if the
// connection's character set is a legacy multi-byte encoding where a
// trailing lead byte can swallow the escaping backslash that follows it
// (classic "GBK injection"); TiDB's ASCII-compatible encodings (utf8,
// utf8mb4, latin1, binary) do not have this problem.
func sqlStringLiteral(s string) (string, error) {
	escaped := strings.ReplaceAll(s, "\\", "\\\\")
	escaped = strings.ReplaceAll(escaped, "'", "\\'")
	return "'" + escaped + "'", nil
}

// sqlValueLiteral formats a bound arg as a SQL literal suitable for a
// "SET @v = <literal>" assignment, mirroring the small set of types
// database/sql itself accepts as driver.Value / commonly-passed Go types,
// plus any type implementing driver.Valuer (e.g. sql.NullString,
// sql.NullInt64, or a custom column type), whose returned driver.Value is
// formatted the same way.
func sqlValueLiteral(v any) (string, error) {
	if dv, ok := v.(driver.Valuer); ok {
		val, err := dv.Value()
		if err != nil {
			return "", fmt.Errorf("driver.Valuer.Value: %w", err)
		}
		return sqlValueLiteral(val)
	}

	switch x := v.(type) {
	case nil:
		return "NULL", nil
	case string:
		return sqlStringLiteral(x)
	case []byte:
		return sqlStringLiteral(string(x))
	case bool:
		if x {
			return "1", nil
		}
		return "0", nil
	case int:
		return strconv.Itoa(x), nil
	case int8:
		return strconv.FormatInt(int64(x), 10), nil
	case int16:
		return strconv.FormatInt(int64(x), 10), nil
	case int32:
		return strconv.FormatInt(int64(x), 10), nil
	case int64:
		return strconv.FormatInt(x, 10), nil
	case uint:
		return strconv.FormatUint(uint64(x), 10), nil
	case uint8:
		return strconv.FormatUint(uint64(x), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(x), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(x), 10), nil
	case uint64:
		return strconv.FormatUint(x, 10), nil
	case float32:
		return strconv.FormatFloat(float64(x), 'g', -1, 32), nil
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64), nil
	case time.Time:
		s, _ := sqlStringLiteral(x.Format("2006-01-02 15:04:05.000000"))
		return s, nil
	default:
		return "", fmt.Errorf("unsupported arg type %T", v)
	}
}
