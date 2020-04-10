package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/jackc/pgx"
)

var connectionString = flag.String("conn", "postgresql://root@localhost:26257/defaultdb?sslmode=disable", "cockroach connection string")
var port = flag.Int("port", 26258, "http server listening port")

var sourceTable = flag.String("source_table", "", "Name of the source table sending data")

var resultDB = flag.String("db", "defaultdb", "database for the receiving table")
var resultTable = flag.String("table", "", "receiving table, must exist")

var sinkDB = flag.String("sink_db", "_CDC_SINK", "db for storing temp sink tables")

var dropDB = flag.Bool("drop", false, "Drop the sink db before starting?")

func createHandler(conn *pgx.Conn, sinks *Sinks) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		log.Printf("Request: %s", r.RequestURI)
		log.Printf("Header: %s", r.Header)

		// Is it an ndjson url?
		ndjson, ndjsonErr := parseNdjsonURL(r.RequestURI)
		if ndjsonErr == nil {
			sink := sinks.FindSink(ndjson.topic)
			if sink != nil {
				log.Printf("Found Sink: %s", sink.originalTableName)
				sink.HandleRequest(ctx, conn, w, r)
				return
			}

			// No sink found, throw an error.
			fmt.Fprintf(w, "could not find a sync for %s", ndjson.topic)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// Is it a resolved url?
		resolved, resolvedErr := parseResolvedURL(r.RequestURI)
		if resolvedErr == nil {
			sinks.HandleResolvedRequest(ctx, conn, resolved, w, r)
			return
		}

		// Could not recognize url.
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "URL pattern does not match either an ndjson (%s) or a resolved (%s)",
			ndjsonErr, resolvedErr,
		)
		return
	}
}

func main() {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, *connectionString)
	if err != nil {
		log.Fatal("error connecting to the database: ", err)
	}
	defer conn.Close(context.Background())

	if *dropDB {
		if err := DropSinkDB(ctx, conn); err != nil {
			log.Fatal(err)
		}
	}

	if err := CreateSinkDB(ctx, conn); err != nil {
		log.Fatal(err)
	}

	sinks := CreateSinks()

	// Add all the sinks here
	if err := sinks.AddSink(ctx, conn, *sourceTable, *resultDB, *resultTable); err != nil {
		log.Fatal(err)
	}

	handler := createHandler(conn, sinks)
	http.HandleFunc("/", handler)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), nil))
}
