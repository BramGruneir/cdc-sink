package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"

	"github.com/jackc/pgx"
)

// Sinks holds a map of all known sinks.
type Sinks struct {
	// Is this mutex overkill?  Is it needed?
	sync.RWMutex
	sinks map[string]*Sink
}

// CreateSinks creates a new table sink.
func CreateSinks() *Sinks {
	return &Sinks{
		sinks: make(map[string]*Sink),
	}
}

// AddSink creates and adds a new sink to the sinks map.
func (s *Sinks) AddSink(
	ctx context.Context, conn *pgx.Conn, originalTable string, resultDB string, resultTable string,
) error {
	s.Lock()
	defer s.Unlock()

	originalTableLower := strings.ToLower(originalTable)
	resultDBLower := strings.ToLower(resultDB)
	resultTableLower := strings.ToLower(resultTable)

	sink, err := CreateSink(ctx, conn, originalTableLower, resultDBLower, resultTableLower)
	if err != nil {
		return err
	}

	s.sinks[originalTableLower] = sink
	return nil
}

// FindSink returns a sink for a given table name.
func (s *Sinks) FindSink(table string) *Sink {
	s.RLock()
	defer s.RUnlock()
	result, _ := s.sinks[table]
	return result
}

// GetAllSinks gets a list of all known sinks.
func (s *Sinks) GetAllSinks() []*Sink {
	s.RLock()
	defer s.RUnlock()
	var allSinks []*Sink
	for _, sink := range s.sinks {
		allSinks = append(allSinks, sink)
	}
	return allSinks
}

// HandleResolvedRequest parses and applies all the resolved upserts.
func (s *Sinks) HandleResolvedRequest(
	ctx context.Context, conn *pgx.Conn, rURL resolvedURL, w http.ResponseWriter, r *http.Request,
) {
	scanner := bufio.NewScanner(r.Body)
	defer r.Body.Close()
	for scanner.Scan() {
		next, err := parseResolvedLine(scanner.Bytes(), rURL.endpoint)
		if err != nil {
			log.Print(err)
			fmt.Fprint(w, err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		log.Printf("Current Resolved: %+v", next)

		// Start the transation
		tx, err := conn.Begin(ctx)
		if err != nil {
			log.Print(err)
			fmt.Fprint(w, err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		defer tx.Rollback(ctx)

		// Get the previous resolved
		prev, err := getPreviousResolved(ctx, tx, rURL.endpoint)
		if err != nil {
			log.Print(err)
			fmt.Fprint(w, err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		log.Printf("Previous Resolved: %+v", prev)

		// Find all rows to update and upsert them.
		allSinks := s.GetAllSinks()
		for _, sink := range allSinks {
			if err := sink.UpdateRows(ctx, tx, prev, next); err != nil {
				log.Print(err)
				fmt.Fprint(w, err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
		}

		// Write the updated resolved.
		if err := next.writeUpdated(ctx, tx); err != nil {
			log.Print(err)
			fmt.Fprint(w, err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// Needs Retry.
		if err := tx.Commit(ctx); err != nil {
			log.Print(err)
			fmt.Fprint(w, err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	}
}
