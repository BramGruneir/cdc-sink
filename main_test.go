package main

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jackc/pgx"
)

// These test require an insecure cockroach server is running on the default
// port with the default root user with no password.

var r *rand.Rand

func init() {
	r = rand.New(rand.NewSource(time.Now().UnixNano()))
}

// getDB creates a new testing DB, return the name of that db and a closer that
// will drop the table and close the db connection.
func getDB(ctx context.Context, t *testing.T) (conn *pgx.Conn, dbName string, closer func()) {
	var err error
	conn, err = pgx.Connect(ctx, *connectionString)
	if err != nil {
		t.Fatal(err)
	}

	// Create the testing database
	dbNum := r.Intn(10000)
	dbName = fmt.Sprintf("_test_db_%d", dbNum)

	t.Logf("Testing Database: %s", dbName)

	if _, err := conn.Exec(ctx,
		`CREATE DATABASE IF NOT EXISTS `+dbName); err != nil {
		t.Fatal(err)
	}

	closer = func() {
		if _, err := conn.Exec(ctx, fmt.Sprintf("DROP DATABASE %s CASCADE", dbName)); err != nil {
			t.Fatal(err)
		}
		conn.Close(ctx)
	}

	return
}

func getRowCount(ctx context.Context, t *testing.T, conn *pgx.Conn, fullTableName string) int {
	var count int
	if err := conn.QueryRow(ctx, "SELECT COUNT(*) FROM "+fullTableName).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

type tableInfo struct {
	conn   *pgx.Conn
	dbName string
	name   string
}

func (ti tableInfo) getFullName() string {
	return fmt.Sprintf("%s.%s", ti.dbName, ti.name)
}

func (ti *tableInfo) deleteAll(ctx context.Context, t *testing.T) {
	if _, err := ti.conn.Exec(
		ctx, fmt.Sprintf("DELETE FROM %s WHERE true", ti.getFullName()),
	); err != nil {
		t.Fatal(err)
	}
}

func (ti tableInfo) getTableRowCount(ctx context.Context, t *testing.T) int {
	return getRowCount(ctx, t, ti.conn, ti.getFullName())
}

func (ti tableInfo) dropTable(ctx context.Context, t *testing.T) {
	if _, err := ti.conn.Exec(
		ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", ti.getFullName()),
	); err != nil {
		t.Fatal(err)
	}
}

// This function creates a test table and returns its name.
func createTestTable(
	ctx context.Context, t *testing.T, conn *pgx.Conn, dbName string, schema string,
) tableInfo {
	var tableName string

outer:
	for {
		// Create the testing database
		tableNum := r.Intn(10000)
		tableName = fmt.Sprintf("_test_table_%d", tableNum)

		// Find the DB.
		var actualTableName string
		err := conn.QueryRow(ctx,
			fmt.Sprintf("SELECT table_name FROM [SHOW TABLES FROM %s] WHERE table_name = $1", dbName),
			tableName,
		).Scan(&actualTableName)
		switch err {
		case pgx.ErrNoRows:
			break outer
		case nil:
			continue
		default:
			t.Fatal(err)
		}
	}

	if _, err := conn.Exec(ctx,
		fmt.Sprintf(tableSimpleSchema, dbName, tableName)); err != nil {
		t.Fatal(err)
	}

	t.Logf("Testing Table: %s.%s", dbName, tableName)
	return tableInfo{
		conn:   conn,
		dbName: dbName,
		name:   tableName,
	}
}

type tableInfoSimple struct {
	tableInfo
	rowCount int
}

const tableSimpleSchema = `
CREATE TABLE %s.%s (
	a INT PRIMARY KEY,
	b INT
)
`

func createTestSimpleTable(
	ctx context.Context, t *testing.T, conn *pgx.Conn, dbName string,
) tableInfoSimple {
	return tableInfoSimple{
		tableInfo: createTestTable(ctx, t, conn, dbName, tableSimpleSchema),
	}
}

func (tis *tableInfoSimple) populateTable(ctx context.Context, t *testing.T, count int) {
	for i := 0; i < count; i++ {
		if _, err := tis.conn.Exec(ctx,
			fmt.Sprintf("INSERT INTO %s VALUES ($1, $1)", tis.getFullName()),
			tis.rowCount+1,
		); err != nil {
			t.Fatal(err)
		}
		tis.rowCount++
	}
}

func (tis *tableInfoSimple) updateNoneKeyColumns(ctx context.Context, t *testing.T) {
	if _, err := tis.conn.Exec(ctx,
		fmt.Sprintf("UPDATE %s SET b=b*100 WHERE true", tis.getFullName()),
	); err != nil {
		t.Fatal(err)
	}
}

func (tis *tableInfoSimple) updateAll(ctx context.Context, t *testing.T) {
	if _, err := tis.conn.Exec(ctx,
		fmt.Sprintf("UPDATE %s SET a=a*100, b=b*100 WHERE true", tis.getFullName()),
	); err != nil {
		t.Fatal(err)
	}
}

func (tis *tableInfoSimple) maxB(ctx context.Context, t *testing.T) int {
	var max int
	if err := tis.conn.QueryRow(ctx,
		fmt.Sprintf("SELECT max(b) FROM %s", tis.getFullName()),
	).Scan(&max); err != nil {
		t.Fatal(err)
	}
	return max
}

type jobInfo struct {
	conn *pgx.Conn
	id   int
}

func (ji *jobInfo) cancelJob(ctx context.Context, t *testing.T) {
	if ji.id == 0 {
		return
	}
	if _, err := ji.conn.Exec(ctx, fmt.Sprintf("CANCEL JOB %d", ji.id)); err != nil {
		t.Fatal(err)
	}
	ji.id = 0
}

func createChangeFeed(
	ctx context.Context, t *testing.T, conn *pgx.Conn, url string, tis ...tableInfo,
) jobInfo {
	query := "CREATE CHANGEFEED FOR TABLE "
	for i := 0; i < len(tis); i++ {
		if i != 0 {
			query += fmt.Sprintf(", ")
		}
		query += fmt.Sprintf(tis[i].getFullName())
	}
	query += fmt.Sprintf(" INTO 'experimental-%s/test.sql' WITH updated,resolved", url)
	var jobID int
	if err := conn.QueryRow(ctx, query).Scan(&jobID); err != nil {
		t.Fatal(err)
	}
	return jobInfo{
		conn: conn,
		id:   jobID,
	}
}

// dropSinkDB is just a wrapper around DropSinkDB for testing.
func dropSinkDB(ctx context.Context, t *testing.T, conn *pgx.Conn) {
	if err := DropSinkDB(ctx, conn); err != nil {
		t.Fatal(err)
	}
}

// createSinkDB will first drop then create a new sink db.
func createSinkDB(ctx context.Context, t *testing.T, conn *pgx.Conn) {
	dropSinkDB(ctx, t, conn)
	if err := CreateSinkDB(ctx, conn); err != nil {
		t.Fatal(err)
	}
}

// TestDB is just a quick test to create and drop a database to ensure the
// Cockroach Cluster is working correctly and we have the correct permissions.
func TestDB(t *testing.T) {
	ctx := context.Background()
	conn, dbName, dbClose := getDB(ctx, t)
	defer dbClose()

	// Find the DB.
	var actualDBName string
	if err := conn.QueryRow(ctx,
		`SELECT database_name FROM [SHOW DATABASES] WHERE database_name = $1`, dbName,
	).Scan(&actualDBName); err != nil {
		t.Fatal(err)
	}

	if actualDBName != dbName {
		t.Fatal(fmt.Sprintf("DB names do not match expected - %s, actual: %s", dbName, actualDBName))
	}

	// Create a test table and insert some rows
	table := createTestSimpleTable(ctx, t, conn, dbName)
	table.populateTable(ctx, t, 10)
	if count := table.getTableRowCount(ctx, t); count != 10 {
		t.Fatalf("Expected Rows 10, actual %d", count)
	}
}

func TestFeedInsert(t *testing.T) {
	// Create the test db
	ctx := context.Background()
	conn, dbName, dbClose := getDB(ctx, t)
	defer dbClose()

	// Create a new _cdc_sink db
	createSinkDB(ctx, t, conn)
	defer dropSinkDB(ctx, t, conn)

	// Create the table to import from
	tableFrom := createTestSimpleTable(ctx, t, conn, dbName)

	// Create the table to receive into
	tableTo := createTestSimpleTable(ctx, t, conn, dbName)

	// Give the from table a few rows
	tableFrom.populateTable(ctx, t, 10)
	if count := tableFrom.getTableRowCount(ctx, t); count != 10 {
		t.Fatalf("Expected Rows 10, actual %d", count)
	}

	// Create the sinks and sink
	sinks := CreateSinks()
	if err := sinks.AddSink(ctx, conn, tableFrom.name, tableTo.dbName, tableTo.name); err != nil {
		t.Fatal(err)
	}

	// Create a test http server
	handler := createHandler(conn, sinks)
	server := httptest.NewServer(
		http.HandlerFunc(handler),
	)
	defer server.Close()
	t.Log(server.URL)

	job := createChangeFeed(ctx, t, conn, server.URL, tableFrom.tableInfo)
	defer job.cancelJob(ctx, t)

	tableFrom.populateTable(ctx, t, 10)

	for tableTo.getTableRowCount(ctx, t) != 20 {
		// add a stopper here from a wrapper around the handler.
		time.Sleep(time.Millisecond * 100)
	}

	tableFrom.populateTable(ctx, t, 10)

	for tableTo.getTableRowCount(ctx, t) != 30 {
		// add a stopper here from a wrapper around the handler.
		time.Sleep(time.Millisecond * 100)
	}

	// Make sure sink table is empty here.
	sink := sinks.FindSink(tableFrom.name)
	if sinkCount := getRowCount(ctx, t, conn, sink.sinkTableFullName); sinkCount != 0 {
		t.Fatalf("expect no rows in the sink table, found %d", sinkCount)
	}
}

func TestFeedDelete(t *testing.T) {
	// Create the test db
	ctx := context.Background()
	conn, dbName, dbClose := getDB(ctx, t)
	defer dbClose()

	// Create a new _cdc_sink db
	createSinkDB(ctx, t, conn)
	defer dropSinkDB(ctx, t, conn)

	// Create the table to import from
	tableFrom := createTestSimpleTable(ctx, t, conn, dbName)

	// Create the table to receive into
	tableTo := createTestSimpleTable(ctx, t, conn, dbName)

	// Give the from table a few rows
	tableFrom.populateTable(ctx, t, 10)
	if count := tableFrom.getTableRowCount(ctx, t); count != 10 {
		t.Fatalf("Expected Rows 10, actual %d", count)
	}

	// Create the sinks and sink
	sinks := CreateSinks()
	if err := sinks.AddSink(ctx, conn, tableFrom.name, tableTo.dbName, tableTo.name); err != nil {
		t.Fatal(err)
	}

	// Create a test http server
	handler := createHandler(conn, sinks)
	server := httptest.NewServer(
		http.HandlerFunc(handler),
	)
	defer server.Close()
	t.Log(server.URL)

	job := createChangeFeed(ctx, t, conn, server.URL, tableFrom.tableInfo)
	defer job.cancelJob(ctx, t)

	tableFrom.populateTable(ctx, t, 10)

	for tableTo.getTableRowCount(ctx, t) != 20 {
		// add a stopper here from a wrapper around the handler.
		time.Sleep(time.Millisecond * 100)
	}

	tableFrom.deleteAll(ctx, t)

	for tableTo.getTableRowCount(ctx, t) != 0 {
		// add a stopper here from a wrapper around the handler.
		time.Sleep(time.Millisecond * 100)
	}

	// Make sure sink table is empty here.
	sink := sinks.FindSink(tableFrom.name)
	if sinkCount := getRowCount(ctx, t, conn, sink.sinkTableFullName); sinkCount != 0 {
		t.Fatalf("expect no rows in the sink table, found %d", sinkCount)
	}
}

func TestFeedUpdateNonPrimary(t *testing.T) {
	// Create the test db
	ctx := context.Background()
	conn, dbName, dbClose := getDB(ctx, t)
	defer dbClose()

	// Create a new _cdc_sink db
	createSinkDB(ctx, t, conn)
	defer dropSinkDB(ctx, t, conn)

	// Create the table to import from
	tableFrom := createTestSimpleTable(ctx, t, conn, dbName)

	// Create the table to receive into
	tableTo := createTestSimpleTable(ctx, t, conn, dbName)

	// Give the from table a few rows
	tableFrom.populateTable(ctx, t, 10)
	if count := tableFrom.getTableRowCount(ctx, t); count != 10 {
		t.Fatalf("Expected Rows 10, actual %d", count)
	}

	// Create the sinks and sink
	sinks := CreateSinks()
	if err := sinks.AddSink(ctx, conn, tableFrom.name, tableTo.dbName, tableTo.name); err != nil {
		t.Fatal(err)
	}

	// Create a test http server
	handler := createHandler(conn, sinks)
	server := httptest.NewServer(
		http.HandlerFunc(handler),
	)
	defer server.Close()
	t.Log(server.URL)

	job := createChangeFeed(ctx, t, conn, server.URL, tableFrom.tableInfo)
	defer job.cancelJob(ctx, t)

	tableFrom.populateTable(ctx, t, 10)

	for tableTo.getTableRowCount(ctx, t) != 20 {
		// add a stopper here from a wrapper around the handler.
		time.Sleep(time.Millisecond * 100)
	}

	tableFrom.updateNoneKeyColumns(ctx, t)

	for tableTo.maxB(ctx, t) != 2000 {
		// add a stopper here from a wrapper around the handler.
		time.Sleep(time.Millisecond * 100)
	}

	// Make sure sink table is empty here.
	sink := sinks.FindSink(tableFrom.name)
	if sinkCount := getRowCount(ctx, t, conn, sink.sinkTableFullName); sinkCount != 0 {
		t.Fatalf("expect no rows in the sink table, found %d", sinkCount)
	}
}

func TestFeedUpdatePrimary(t *testing.T) {
	// Create the test db
	ctx := context.Background()
	conn, dbName, dbClose := getDB(ctx, t)
	defer dbClose()

	// Create a new _cdc_sink db
	createSinkDB(ctx, t, conn)
	defer dropSinkDB(ctx, t, conn)

	// Create the table to import from
	tableFrom := createTestSimpleTable(ctx, t, conn, dbName)

	// Create the table to receive into
	tableTo := createTestSimpleTable(ctx, t, conn, dbName)

	// Give the from table a few rows
	tableFrom.populateTable(ctx, t, 10)
	if count := tableFrom.getTableRowCount(ctx, t); count != 10 {
		t.Fatalf("Expected Rows 10, actual %d", count)
	}

	// Create the sinks and sink
	sinks := CreateSinks()
	if err := sinks.AddSink(ctx, conn, tableFrom.name, tableTo.dbName, tableTo.name); err != nil {
		t.Fatal(err)
	}

	// Create a test http server
	handler := createHandler(conn, sinks)
	server := httptest.NewServer(
		http.HandlerFunc(handler),
	)
	defer server.Close()
	t.Log(server.URL)

	job := createChangeFeed(ctx, t, conn, server.URL, tableFrom.tableInfo)
	defer job.cancelJob(ctx, t)

	tableFrom.populateTable(ctx, t, 10)

	for tableTo.getTableRowCount(ctx, t) != 20 {
		// add a stopper here from a wrapper around the handler.
		time.Sleep(time.Millisecond * 100)
	}

	tableFrom.updateAll(ctx, t)

	for tableTo.maxB(ctx, t) != 2000 {
		// add a stopper here from a wrapper around the handler.
		time.Sleep(time.Millisecond * 100)
	}

	// Make sure sink table is empty here.
	sink := sinks.FindSink(tableFrom.name)
	if sinkCount := getRowCount(ctx, t, conn, sink.sinkTableFullName); sinkCount != 0 {
		t.Fatalf("expect no rows in the sink table, found %d", sinkCount)
	}
}

func TestTypes(t *testing.T) {
	// Create the test db
	ctx := context.Background()
	conn, dbName, dbClose := getDB(ctx, t)
	defer dbClose()

	// Create a new _cdc_sink db
	createSinkDB(ctx, t, conn)
	defer dropSinkDB(ctx, t, conn)

	// Create the sinks
	sinks := CreateSinks()

	// Create a test http server
	handler := createHandler(conn, sinks)
	server := httptest.NewServer(
		http.HandlerFunc(handler),
	)
	defer server.Close()
	t.Log(server.URL)

	testcases := []struct {
		name        string
		columnType  string
		columnValue string
		indexable   bool
	}{
		// {`array`, `STRING[]`, `{"sky","road","car"}`, false}, -- sql: converting argument $1 type: unsupported type []interface {}, a slice of interface
		{`bit`, `VARBIT`, `10010101`, true},
		{`bool`, `BOOL`, `true`, true},
		// {`bytes`, `BYTES`, `b'\141\061\142\062\143\063'`, true}, -- error on cdc-sink side
		// {`collate`, `COLLATE`, `'a1b2c3' COLLATE en`, true}, -- test not implemented yet
		{`date`, `DATE`, `2016-01-25`, true},
		{`decimal`, `DECIMAL`, `1.2345`, true},
		{`float`, `FLOAT`, `1.2345`, true},
		{`inet`, `INET`, `192.168.0.1`, true},
		{`int`, `INT`, `12345`, true},
		{`interval`, `INTERVAL`, `2h30m30s`, true},
		// {`jsonb`, `JSONB`, `'{"first_name": "Lola", "last_name": "Dog", "location": "NYC", "online" : true, "friends" : 547}'`, false}, -- error in test, maybe pgx?
		// {`serial`, `SERIAL`, `148591304110702593`, true}, -- error on cdc-sink side?
		{`string`, `STRING`, `a1b2c3`, true},
		{`time`, `TIME`, `01:23:45.123456`, true},
		{`timestamp`, `TIMESTAMP`, `2016-01-25 10:10:10`, true},
		{`timestamptz`, `TIMESTAMPTZ`, `2016-01-25 10:10:10-05:00`, true},
		{`uuid`, `UUID`, `7f9c24e8-3b12-4fef-91e0-56a2d5a246ec`, true},
	}

	tableIndexableSchema := `CREATE TABLE %s (a %s PRIMARY KEY,	b %s)`
	tableNonIndexableSchema := `CREATE TABLE %s (a INT PRIMARY KEY,	b %s)`

	for _, test := range testcases {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			tableIn := tableInfo{
				conn:   conn,
				dbName: dbName,
				name:   fmt.Sprintf("in_%s", test.name),
			}
			tableOut := tableInfo{
				conn:   conn,
				dbName: dbName,
				name:   fmt.Sprintf("out_%s", test.name),
			}

			// Drop both tables if they already exist.
			tableIn.dropTable(ctx, t)
			tableOut.dropTable(ctx, t)

			// Create both tables.
			if test.indexable {
				if _, err := conn.Exec(ctx,
					fmt.Sprintf(tableIndexableSchema,
						tableIn.getFullName(), test.columnType, test.columnType,
					)); err != nil {
					t.Fatal(err)
				}
				if _, err := conn.Exec(ctx,
					fmt.Sprintf(tableIndexableSchema,
						tableOut.getFullName(), test.columnType, test.columnType,
					)); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := conn.Exec(ctx,
					fmt.Sprintf(tableNonIndexableSchema,
						tableIn.getFullName(), test.columnType,
					)); err != nil {
					t.Fatal(err)
				}
				if _, err := conn.Exec(ctx,
					fmt.Sprintf(tableNonIndexableSchema,
						tableOut.getFullName(), test.columnType,
					)); err != nil {
					t.Fatal(err)
				}
			}

			// Defer a table drop for both tables to clean them up.
			defer tableIn.dropTable(ctx, t)
			defer tableOut.dropTable(ctx, t)

			// Create the sink
			// There is no way to remove a sink at this time, and that should be ok
			// for these tests.
			if err := sinks.AddSink(ctx, conn, tableIn.name, dbName, tableOut.name); err != nil {
				t.Fatal(err)
			}

			// Create the CDC feed.
			job := createChangeFeed(ctx, t, conn, server.URL, tableIn)
			defer job.cancelJob(ctx, t)

			// Insert a row into the in table.
			if test.indexable {
				if _, err := conn.Exec(ctx,
					fmt.Sprintf("INSERT INTO %s (a,b) VALUES ($1,$2)", tableIn.getFullName()),
					test.columnValue, test.columnValue,
				); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := conn.Exec(ctx,
					fmt.Sprintf("INSERT INTO %s (a, b) VALUES (1, $1)", tableIn.getFullName()),
					test.columnValue,
				); err != nil {
					t.Fatal(err)
				}
			}

			// Wait until the out table has a row.
			for tableOut.getTableRowCount(ctx, t) != 1 {
				// add a stopper here from a wrapper around the handler.
				time.Sleep(time.Millisecond * 100)
			}

			// Now fetch that rows and compare them.
			var inA, inB interface{}
			if err := conn.QueryRow(ctx,
				fmt.Sprintf("SELECT a, b FROM %s LIMIT 1", tableIn.getFullName()),
			).Scan(&inA, &inB); err != nil {
				t.Fatal(err)
			}
			var outA, outB interface{}
			if err := conn.QueryRow(ctx,
				fmt.Sprintf("SELECT a, b FROM %s LIMIT 1", tableOut.getFullName()),
			).Scan(&outA, &outB); err != nil {
				t.Fatal(err)
			}
			if fmt.Sprintf("%v", inA) != fmt.Sprintf("%v", outA) {
				t.Errorf("A: expected %v, got %v", inA, outA)
			}
			if fmt.Sprintf("%v", inB) != fmt.Sprintf("%v", outB) {
				t.Errorf("B: expected %v, got %v", inB, outB)
			}
		})
	}
}
