package multistmt

import (
	"strings"
	"testing"
)

func TestBuildSetOnlySQL(t *testing.T) {
	b := New()
	b.Add("begin", nil, false, nil)
	b.Add("SELECT c FROM sbtest1 WHERE id = ?", []any{42}, true, nil)
	b.Add("commit", nil, false, nil)

	sql, err := b.BuildSetOnlySQL()
	if err != nil {
		t.Fatalf("BuildSetOnlySQL failed: %v", err)
	}

	if strings.Contains(sql, "PREPARE") || strings.Contains(sql, "EXECUTE") || strings.Contains(sql, "DEALLOCATE") {
		t.Fatalf("expected no PREPARE/EXECUTE/DEALLOCATE in %q", sql)
	}
	if !strings.HasPrefix(sql, "SET @_multistmt_statement_num=0;") {
		t.Fatalf("expected leading reset marker in %q", sql)
	}
	if !strings.Contains(sql, "SET @_multistmt_statement_num=1;") {
		t.Fatalf("expected bare marker for no-arg begin in %q", sql)
	}
	if !strings.Contains(sql, "SET @_multistmt_statement_num=2, @_multistmt_") || !strings.Contains(sql, "_0=42;") {
		t.Fatalf("expected merged marker+arg SET for the SELECT in %q", sql)
	}
	if !strings.Contains(sql, "SET @_multistmt_statement_num=3;") {
		t.Fatalf("expected bare marker for no-arg commit in %q", sql)
	}

	// Matches the real batch's statement count exactly: one SET per
	// statement plus the leading reset, nothing more, nothing less.
	if got := strings.Count(sql, "SET "); got != 4 {
		t.Fatalf("expected 4 SET statements (reset + 3 markers), got %d in %q", got, sql)
	}
}

func TestBuildSetOnlySQL_Empty(t *testing.T) {
	b := New()
	sql, err := b.BuildSetOnlySQL()
	if err != nil {
		t.Fatalf("BuildSetOnlySQL failed: %v", err)
	}
	if sql != "SET @_multistmt_statement_num=0;" {
		t.Fatalf("expected just the reset marker for an empty batch, got %q", sql)
	}
}
