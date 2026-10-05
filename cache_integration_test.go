package multistmt

// Integration tests against a real TiDB/MySQL for Cache specifically.
// Skipped unless MULTISTMT_TEST_DSN is set — see integration_test.go.
//
// This file is in package multistmt (not multistmt_test) because it needs
// connIdentity and Cache's internal maps to assert the thing that actually
// matters: that the *physical* connection identity — not the *sql.Conn Go
// wrapper — stays stable across database/sql pool checkouts, and that a
// statement PREPAREd through one checkout is still recognized and reused
// (not re-PREPAREd) through a later, different *sql.Conn wrapping the same
// physical connection.

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

func testDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("MULTISTMT_TEST_DSN")
	if dsn == "" {
		t.Skip("MULTISTMT_TEST_DSN not set, skipping integration test")
	}
	return dsn
}

func TestIntegration_SameConnIdentityAcrossPoolCheckout(t *testing.T) {
	dsn := testDSN(t)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	ctx := context.Background()

	conn1, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("db.Conn (1st checkout): %v", err)
	}
	id1, err := connIdentity(conn1)
	if err != nil {
		t.Fatalf("connIdentity (1st checkout): %v", err)
	}
	if err := conn1.Close(); err != nil {
		t.Fatalf("conn1.Close: %v", err)
	}

	conn2, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("db.Conn (2nd checkout): %v", err)
	}
	defer conn2.Close()
	id2, err := connIdentity(conn2)
	if err != nil {
		t.Fatalf("connIdentity (2nd checkout): %v", err)
	}

	if id1 != id2 {
		t.Fatalf("expected the same physical connection identity across pool checkouts (MaxOpenConns=1), got %v then %v", id1, id2)
	}
}

func TestIntegration_PreparedCacheReusesServerSidePrepareAcrossCheckout(t *testing.T) {
	dsn := testDSN(t)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	ctx := context.Background()

	conn1, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("db.Conn: %v", err)
	}
	if _, err := conn1.ExecContext(ctx, "DROP TABLE IF EXISTS multistmt_cache_it"); err != nil {
		t.Fatalf("drop table: %v", err)
	}
	if _, err := conn1.ExecContext(ctx, "CREATE TABLE multistmt_cache_it (id INT PRIMARY KEY, name VARCHAR(50))"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := conn1.ExecContext(ctx, "INSERT INTO multistmt_cache_it (id, name) VALUES (1, 'first')"); err != nil {
		t.Fatalf("seed insert: %v", err)
	}
	defer func() {
		cleanupConn, err := db.Conn(context.Background())
		if err == nil {
			cleanupConn.ExecContext(context.Background(), "DROP TABLE IF EXISTS multistmt_cache_it")
			cleanupConn.Close()
		}
	}()

	cache := NewPreparedCache(0, 0)
	const q = "SELECT name FROM multistmt_cache_it WHERE id = ?"

	var gotName string
	b1 := New()
	b1.Add(q, []any{1}, true, func(r *StatementResult) {
		if r.Err != nil {
			return
		}
		if r.Rows.Next() {
			r.Rows.Scan(&gotName)
		}
	})
	if err := b1.Execute(ctx, conn1, WithPreparedCache(cache)); err != nil {
		t.Fatalf("Execute+WithPreparedCache (1st checkout, expected a fresh PREPARE): %v", err)
	}
	if gotName != "first" {
		t.Fatalf("expected to read back 'first', got %q", gotName)
	}

	id1, err := connIdentity(conn1)
	if err != nil {
		t.Fatalf("connIdentity: %v", err)
	}
	cc := cache.conns[id1]
	if cc == nil || len(cc.stmts) != 1 {
		t.Fatalf("expected exactly one cached statement for this connection after the first Execute+WithPreparedCache, got %+v", cc)
	}
	cachedName := cc.stmts[q].Value.(*stmtEntry).name

	if err := conn1.Close(); err != nil {
		t.Fatalf("conn1.Close: %v", err)
	}

	conn2, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("db.Conn (2nd checkout): %v", err)
	}
	defer conn2.Close()

	id2, err := connIdentity(conn2)
	if err != nil {
		t.Fatalf("connIdentity: %v", err)
	}
	if id1 != id2 {
		t.Fatalf("expected the same physical connection identity across checkouts (MaxOpenConns=1), got %v then %v", id1, id2)
	}

	var gotName2 string
	b2 := New()
	b2.Add(q, []any{1}, true, func(r *StatementResult) {
		if r.Err != nil {
			return
		}
		if r.Rows.Next() {
			r.Rows.Scan(&gotName2)
		}
	})
	// If Cache failed to recognize conn2 as the same physical connection as
	// conn1 and still decided to skip PREPARE (or, conversely, if the
	// server-side PREPARE hadn't really survived the checkout the way this
	// package assumes), this EXECUTE-without-PREPARE would come back from a
	// real TiDB as "Unknown prepared statement handler" — not a local
	// assertion failure.
	if err := b2.Execute(ctx, conn2, WithPreparedCache(cache)); err != nil {
		t.Fatalf("Execute+WithPreparedCache (2nd checkout, expected a cache hit reusing the 1st checkout's PREPARE): %v", err)
	}
	if gotName2 != "first" {
		t.Fatalf("expected to read back 'first' again via the reused PREPARE, got %q", gotName2)
	}

	cc2 := cache.conns[id2]
	if got := cc2.stmts[q].Value.(*stmtEntry).name; got != cachedName {
		t.Fatalf("expected the exact same PREPARE name to be reused across checkouts, got %q then %q", cachedName, got)
	}
}

func TestIntegration_EvictionReallyDeallocatesOnServer(t *testing.T) {
	dsn := testDSN(t)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	ctx := context.Background()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("db.Conn: %v", err)
	}
	defer conn.Close()

	cache := NewPreparedCache(1, 0) // only 1 statement per connection, so stmt 2 evicts stmt 1

	b1 := New()
	b1.Add("SELECT 1", nil, true, func(r *StatementResult) {
		if r.Err != nil {
			t.Fatalf("stmt1 unexpected error: %v", r.Err)
		}
	})
	if err := b1.Execute(ctx, conn, WithPreparedCache(cache)); err != nil {
		t.Fatalf("Execute+WithPreparedCache stmt1: %v", err)
	}

	id, _ := connIdentity(conn)
	evictedName := cache.conns[id].stmts["SELECT 1"].Value.(*stmtEntry).name

	b2 := New()
	b2.Add("SELECT 2", nil, true, func(r *StatementResult) {
		if r.Err != nil {
			t.Fatalf("stmt2 unexpected error: %v", r.Err)
		}
	})
	if err := b2.Execute(ctx, conn, WithPreparedCache(cache)); err != nil {
		t.Fatalf("Execute+WithPreparedCache stmt2 (should evict+deallocate stmt1's PREPARE): %v", err)
	}

	if _, ok := cache.conns[id].stmts["SELECT 1"]; ok {
		t.Fatalf("expected 'SELECT 1' to have been evicted from the cache")
	}

	// The evicted name must really be gone server-side now, not just
	// forgotten locally — EXECUTE-ing it directly must fail.
	_, err = conn.ExecContext(ctx, "EXECUTE "+evictedName)
	if err == nil {
		t.Fatalf("expected the evicted PREPARE name %q to have been DEALLOCATEd server-side, but EXECUTE succeeded", evictedName)
	}
}
