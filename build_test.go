package multistmt

import (
	"strings"
	"testing"
)

func TestBuild_MarkerPrecedesPrepare(t *testing.T) {
	b := New()
	b.Add("SELECT 1", nil, true, nil)
	b.Add("INSERT INTO t VALUES (?)", []any{1}, false, nil)

	built, err := b.build()
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	// The marker update for statement i must appear before that statement's
	// own PREPARE, not after it — otherwise a failure while TiDB compiles
	// the statement's SQL text gets attributed to the previous statement.
	setIdx := strings.Index(built.sql, "SET @_multistmt_statement_num=1;")
	prepIdx := strings.Index(built.sql, "PREPARE _multistmt_ps_")
	if setIdx < 0 || prepIdx < 0 {
		t.Fatalf("expected both a statement_num marker and a PREPARE in %q", built.sql)
	}
	if setIdx > prepIdx {
		t.Fatalf("marker SET must precede PREPARE: marker at %d, PREPARE at %d:\n%s", setIdx, prepIdx, built.sql)
	}
}

func TestBuild_DeallocatesGeneratedNames(t *testing.T) {
	b := New()
	b.Add("SELECT 1", nil, true, nil)
	built, err := b.build()
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}
	if len(built.dealloc) != 1 {
		t.Fatalf("expected exactly one name to deallocate, got %v", built.dealloc)
	}
	if !strings.Contains(built.sql, "DEALLOCATE PREPARE "+built.dealloc[0]) {
		t.Fatalf("expected DEALLOCATE PREPARE for %s in %q", built.dealloc[0], built.sql)
	}
}

func TestBuild_PreparedNameSkipsDeallocation(t *testing.T) {
	b := New()
	b.AddStatement(Statement{SQL: "SELECT 1", HasResultSet: true, PreparedName: "stable_ps"})
	built, err := b.build()
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}
	if len(built.dealloc) != 0 {
		t.Fatalf("PreparedName statement must not be auto-deallocated, got %v", built.dealloc)
	}
	if strings.Contains(built.sql, "DEALLOCATE") {
		t.Fatalf("expected no DEALLOCATE at all in %q", built.sql)
	}
	if !strings.Contains(built.sql, "PREPARE stable_ps FROM") {
		t.Fatalf("expected PREPARE using the given stable name in %q", built.sql)
	}
}

func TestBuild_SkipPrepareOmitsPrepare(t *testing.T) {
	b := New()
	b.AddStatement(Statement{SQL: "SELECT 1", HasResultSet: true, PreparedName: "stable_ps", SkipPrepare: true})
	built, err := b.build()
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}
	if strings.Contains(built.sql, "PREPARE") {
		t.Fatalf("SkipPrepare must omit PREPARE entirely, got %q", built.sql)
	}
	if !strings.Contains(built.sql, "EXECUTE stable_ps") {
		t.Fatalf("expected EXECUTE of the stable name in %q", built.sql)
	}
}

func TestBuild_NoArgsOmitsSetAndUsing(t *testing.T) {
	b := New()
	b.Add("SELECT COUNT(*) FROM t", nil, true, nil)
	built, err := b.build()
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}
	if strings.Contains(built.sql, "USING") {
		t.Fatalf("a statement with no args must not emit USING: %q", built.sql)
	}
}

func TestSQLValueLiteral(t *testing.T) {
	cases := []struct {
		in   any
		want string
	}{
		{nil, "NULL"},
		{"it's", `'it\'s'`},
		{42, "42"},
		{int64(42), "42"},
		{3.5, "3.5"},
		{true, "1"},
		{false, "0"},
	}
	for _, c := range cases {
		got, err := sqlValueLiteral(c.in)
		if err != nil {
			t.Fatalf("sqlValueLiteral(%#v) failed: %v", c.in, err)
		}
		if got != c.want {
			t.Errorf("sqlValueLiteral(%#v) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestSQLValueLiteral_UnsupportedType(t *testing.T) {
	if _, err := sqlValueLiteral(struct{}{}); err == nil {
		t.Fatalf("expected an error for an unsupported arg type")
	}
}

func TestBatchError_Unwrap(t *testing.T) {
	inner := &BatchError{Index: 2, SQL: "SELECT 1", Err: errExample}
	if got := inner.Unwrap(); got != errExample {
		t.Fatalf("Unwrap() = %v, want %v", got, errExample)
	}
}

var errExample = &testError{"boom"}

type testError struct{ s string }

func (e *testError) Error() string { return e.s }
