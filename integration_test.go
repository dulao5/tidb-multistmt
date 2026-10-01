package multistmt_test

// Integration tests against a real TiDB/MySQL. Skipped unless
// MULTISTMT_TEST_DSN is set, e.g.:
//
//	MULTISTMT_TEST_DSN="root:@tcp(127.0.0.1:4000)/multistmt_test?parseTime=true&multiStatements=true" go test ./...
//
// The target database must already exist; these tests create/drop their own
// table inside it.

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"

	"github.com/dulao5/tidb-multistmt"
	_ "github.com/go-sql-driver/mysql"
)

func testConn(t *testing.T) (*sql.Conn, func()) {
	t.Helper()
	dsn := os.Getenv("MULTISTMT_TEST_DSN")
	if dsn == "" {
		t.Skip("MULTISTMT_TEST_DSN not set, skipping integration test")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	db.SetMaxOpenConns(1)
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("db.Conn: %v", err)
	}
	if _, err := conn.ExecContext(ctx, "DROP TABLE IF EXISTS multistmt_it"); err != nil {
		t.Fatalf("drop table: %v", err)
	}
	if _, err := conn.ExecContext(ctx, "CREATE TABLE multistmt_it (id INT PRIMARY KEY, name VARCHAR(50))"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	return conn, func() {
		conn.ExecContext(ctx, "DROP TABLE IF EXISTS multistmt_it")
		conn.Close()
		db.Close()
	}
}

func TestIntegration_AllNonSelectSucceed(t *testing.T) {
	conn, cleanup := testConn(t)
	defer cleanup()
	ctx := context.Background()

	var gotErrs []error
	b := multistmt.New()
	b.Add("INSERT INTO multistmt_it (id, name) VALUES (?, ?)", []any{1, "a"}, false, func(r *multistmt.StatementResult) {
		gotErrs = append(gotErrs, r.Err)
	})
	b.Add("INSERT INTO multistmt_it (id, name) VALUES (?, ?)", []any{2, "b"}, false, func(r *multistmt.StatementResult) {
		gotErrs = append(gotErrs, r.Err)
	})

	if err := b.Execute(ctx, conn); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	for i, e := range gotErrs {
		if e != nil {
			t.Errorf("statement %d: expected nil err, got %v", i, e)
		}
	}

	var count int
	if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM multistmt_it").Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}
}

func TestIntegration_MixedSucceedWithRows(t *testing.T) {
	conn, cleanup := testConn(t)
	defer cleanup()
	ctx := context.Background()

	var selectedName string
	b := multistmt.New()
	b.Add("INSERT INTO multistmt_it (id, name) VALUES (?, ?)", []any{1, "a"}, false, nil)
	b.Add("SELECT name FROM multistmt_it WHERE id = ?", []any{1}, true, func(r *multistmt.StatementResult) {
		if r.Err != nil {
			t.Errorf("select statement failed: %v", r.Err)
			return
		}
		for r.Rows.Next() {
			if err := r.Rows.Scan(&selectedName); err != nil {
				t.Errorf("scan: %v", err)
			}
		}
	})

	if err := b.Execute(ctx, conn); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if selectedName != "a" {
		t.Errorf("expected selectedName=%q, got %q", "a", selectedName)
	}
}

func TestIntegration_FailurePartwaySkipsRest(t *testing.T) {
	conn, cleanup := testConn(t)
	defer cleanup()
	ctx := context.Background()

	var results []*multistmt.StatementResult
	record := func(r *multistmt.StatementResult) { results = append(results, r) }

	b := multistmt.New()
	b.Add("INSERT INTO multistmt_it (id, name) VALUES (?, ?)", []any{1, "a"}, false, record)
	b.Add("INSERT INTO multistmt_it (id, name) VALUES (?, ?)", []any{1, "dup"}, false, record) // fails: duplicate PK
	b.Add("INSERT INTO multistmt_it (id, name) VALUES (?, ?)", []any{2, "b"}, false, record)   // must be skipped

	err := b.Execute(ctx, conn)
	if err == nil {
		t.Fatal("expected Execute to return an error")
	}
	var batchErr *multistmt.BatchError
	if !errors.As(err, &batchErr) {
		t.Fatalf("expected a *multistmt.BatchError, got %T: %v", err, err)
	}
	if batchErr.Index != 1 {
		t.Errorf("expected BatchError.Index=1, got %d", batchErr.Index)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 callback invocations, got %d", len(results))
	}
	if results[0].Err != nil {
		t.Errorf("statement 0 should have succeeded, got %v", results[0].Err)
	}
	if results[1].Err == nil {
		t.Errorf("statement 1 should have failed")
	}
	if !errors.Is(results[2].Err, multistmt.ErrSkipped) {
		t.Errorf("statement 2 should have been skipped, got %v", results[2].Err)
	}

	var count int
	if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM multistmt_it").Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 1 {
		t.Errorf("expected only the first insert to have committed, got %d rows", count)
	}
}

func TestIntegration_SyntaxErrorAttributedToFailingStatement(t *testing.T) {
	conn, cleanup := testConn(t)
	defer cleanup()
	ctx := context.Background()

	var results []*multistmt.StatementResult
	record := func(r *multistmt.StatementResult) { results = append(results, r) }

	b := multistmt.New()
	b.Add("INSERT INTO multistmt_it (id, name) VALUES (?, ?)", []any{1, "ok"}, false, record)
	b.Add("INSERT INTO multistmt_it (id, GARBAGE SYNTAX) VALUES (?, ?)", []any{2, "bad"}, false, record)

	err := b.Execute(ctx, conn)
	var batchErr *multistmt.BatchError
	if !errors.As(err, &batchErr) {
		t.Fatalf("expected a *multistmt.BatchError, got %T: %v", err, err)
	}
	if batchErr.Index != 1 {
		t.Errorf("expected the syntax error to be attributed to statement 1, got %d", batchErr.Index)
	}
	if results[0].Err != nil {
		t.Errorf("statement 0 (well-formed, before the syntax error) should have succeeded, got %v", results[0].Err)
	}

	var count int
	if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM multistmt_it").Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 1 {
		t.Errorf("expected the well-formed leading statement to have committed, got %d rows", count)
	}
}

func TestIntegration_PreparedNameReusedAcrossExecuteCalls(t *testing.T) {
	conn, cleanup := testConn(t)
	defer cleanup()
	ctx := context.Background()
	defer conn.ExecContext(ctx, "DEALLOCATE PREPARE multistmt_it_reused")

	insert := func(id int, name string, skipPrepare bool) error {
		b := multistmt.New()
		b.AddStatement(multistmt.Statement{
			SQL:          "INSERT INTO multistmt_it (id, name) VALUES (?, ?)",
			Args:         []any{id, name},
			PreparedName: "multistmt_it_reused",
			SkipPrepare:  skipPrepare,
		})
		return b.Execute(ctx, conn)
	}

	if err := insert(1, "first", false); err != nil {
		t.Fatalf("first insert (with PREPARE): %v", err)
	}
	if err := insert(2, "second", true); err != nil {
		t.Fatalf("second insert (reusing PREPARE): %v", err)
	}

	var count int
	if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM multistmt_it").Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}
}
