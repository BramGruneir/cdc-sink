package main

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx"
)

// CreateSinkDB creates a new sink db if one does not exist yet and also adds
// the resolved table.
func CreateSinkDB(ctx context.Context, conn *pgx.Conn) error {
	// Needs retry.
	// TODO - Set the zone configs to be small here.
	_, err := conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", *sinkDB))
	if err != nil {
		return err
	}

	return CreateResolvedTable(ctx, conn)
}

// DropSinkDB drops the sinkDB and all data in it.
func DropSinkDB(ctx context.Context, conn *pgx.Conn) error {
	// Needs retry.
	_, err := conn.Exec(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS %s CASCADE`, *sinkDB))
	return err
}

const sqlTableExistsQuery = `SELECT table_name FROM [SHOW TABLES FROM %s] WHERE table_name = '%s'`

// TableExists checks for the existence of a table.
func TableExists(
	ctx context.Context, conn *pgx.Conn, dbName string, tableName string,
) (bool, error) {
	// Needs retry.
	findTableSQL := fmt.Sprintf(sqlTableExistsQuery, dbName, tableName)
	var tableFound string
	err := conn.QueryRow(ctx, findTableSQL).Scan(&tableFound)
	switch err {
	case pgx.ErrNoRows:
		return false, nil
	case nil:
		log.Printf("Found: %s", tableFound)
		return true, nil
	default:
		return false, err
	}
}

const sqlGetPrimaryKeyColumnsQuery = `
SELECT column_name FROM [SHOW INDEX FROM %s] WHERE index_name = 'primary' ORDER BY seq_in_index
`

// GetPrimaryKeyColumns returns the column names for the primary key index for
// a table, in order.
func GetPrimaryKeyColumns(
	ctx context.Context, conn *pgx.Conn, tableFullName string,
) ([]string, error) {
	// Needs retry.
	findKeyColumns := fmt.Sprintf(sqlGetPrimaryKeyColumnsQuery, tableFullName)
	log.Printf(findKeyColumns)
	rows, err := conn.Query(ctx, findKeyColumns)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, err
		}
		columns = append(columns, column)
	}
	log.Printf("Primary Keys for %s: %v", tableFullName, columns)
	return columns, nil
}
