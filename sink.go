package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/jackc/pgx"
)

// Sink holds all the info needed for a specific table.
type Sink struct {
	originalTableName   string
	resultTableFullName string
	sinkTableFullName   string
	primaryKeyColumns   []string
}

// CreateSink creates all the required tables and returns a new Sink.
func CreateSink(
	ctx context.Context, conn *pgx.Conn, originalTable string, resultDB string, resultTable string,
) (*Sink, error) {
	// Check to make sure the table exists.
	resultTableFullName := fmt.Sprintf("%s.%s", resultDB, resultTable)
	exists, err := TableExists(ctx, conn, resultDB, resultTable)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("Table %s could not be found", resultTableFullName)
	}

	sinkTableFullName := SinkTableFullName(resultDB, resultTable)
	if err := CreateSinkTable(ctx, conn, sinkTableFullName); err != nil {
		return nil, err
	}

	columns, err := GetPrimaryKeyColumns(ctx, conn, resultTableFullName)
	if err != nil {
		return nil, err
	}

	sink := &Sink{
		originalTableName:   originalTable,
		resultTableFullName: resultTableFullName,
		sinkTableFullName:   sinkTableFullName,
		primaryKeyColumns:   columns,
	}

	return sink, nil
}

// HandleRequest is a handler used for this specific sink.
func (s *Sink) HandleRequest(
	ctx context.Context, conn *pgx.Conn, w http.ResponseWriter, r *http.Request,
) {
	scanner := bufio.NewScanner(r.Body)
	defer r.Body.Close()
	for scanner.Scan() {
		line, err := parseLine(scanner.Bytes())
		if err != nil {
			log.Print(err)
			fmt.Fprint(w, err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if err := line.WriteToSinkTable(ctx, conn, s.sinkTableFullName); err != nil {
			log.Print(err)
			fmt.Fprint(w, err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	}
}

// deleteRow preforms a delete on a single row.
func (s *Sink) deleteRow(ctx context.Context, tx pgx.Tx, line Line) error {
	// Build the statement.
	var statement strings.Builder
	fmt.Fprintf(&statement, "DELETE FROM %s WHERE ", s.resultTableFullName)
	for i, column := range s.primaryKeyColumns {
		if i > 0 {
			fmt.Fprint(&statement, " AND ")
		}
		// Placeholder index always starts at 1.
		fmt.Fprintf(&statement, "%s = '%v", column, line.Key[i])
	}
	log.Printf("Delete Statement: %s", statement.String())

	// Upsert the line
	_, err := tx.Exec(ctx, statement.String())
	return err
}

// upsertRow performs an upsert on a single row.
func (s *Sink) upsertRow(ctx context.Context, tx pgx.Tx, line Line) error {
	// Parse the after columns
	if err := json.Unmarshal([]byte(line.after), &(line.After)); err != nil {
		return err
	}

	// Find all the columns that need to be part of the upsert statement.
	columns := make(map[string]interface{})
	for name, value := range line.After {
		columns[name] = value
	}
	for i, column := range s.primaryKeyColumns {
		columns[column] = line.Key[i]
	}

	// Build the statement.
	var statement strings.Builder
	fmt.Fprintf(&statement, "UPSERT INTO %s (", s.resultTableFullName)
	var values []interface{}
	for name, value := range columns {
		if len(values) > 0 {
			fmt.Fprint(&statement, ", ")
		}
		fmt.Fprint(&statement, name)
		values = append(values, value)
	}
	fmt.Fprint(&statement, ") VALUES (")
	for i := 0; i < len(values); i++ {
		if i > 0 {
			fmt.Fprint(&statement, ", ")
		}
		// Placeholder index always starts at 1.
		fmt.Fprintf(&statement, "'%v'", values[i])
	}
	fmt.Fprint(&statement, ")")
	log.Printf("Upsert Statement: %s", statement.String())

	// Upsert the line
	_, err := tx.Exec(ctx, statement.String())
	return err
}

// UpdateRows updates all changed rows.
func (s *Sink) UpdateRows(
	ctx context.Context, tx pgx.Tx, prev ResolvedLine, next ResolvedLine,
) error {
	log.Printf("Updating Sink %s", s.resultTableFullName)

	// First, gather all the rows to update.
	lines, err := FindAllRowsToUpdate(ctx, tx, s.sinkTableFullName, prev, next)
	if err != nil {
		return err
	}

	for _, line := range lines {
		log.Printf("line to update: %+v", line)
		// Parse the key into columns
		if err := json.Unmarshal([]byte(line.key), &(line.Key)); err != nil {
			return err
		}
		// Is this needed?  What if we have 2 primary key columns but the 2nd one
		// nullable or has a default?  Does CDC send it?
		if len(line.Key) != len(s.primaryKeyColumns) {
			return fmt.Errorf(
				"table %s has %d primary key columns %v, but only got %d keys %v",
				s.resultTableFullName,
				len(s.primaryKeyColumns),
				s.primaryKeyColumns,
				len(line.Key),
				line.Key,
			)
		}

		// Is this a delete?
		if line.after == "null" {
			if err := s.deleteRow(ctx, tx, line); err != nil {
				return err
			}
			if err := line.DeleteLine(ctx, tx, s.sinkTableFullName); err != nil {
				return err
			}
			continue
		}

		// This can be an upsert statement.
		if err := s.upsertRow(ctx, tx, line); err != nil {
			return err
		}
		if err := line.DeleteLine(ctx, tx, s.sinkTableFullName); err != nil {
			return err
		}
	}

	return nil
}
