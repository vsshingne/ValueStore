// devdb prepares an isolated native test database; never used by production services.
package main

import (
	"context"
	"flag"
	"github.com/jackc/pgx/v5"
	"log"
	"os"
)

func main() {
	reset := flag.Bool("reset", false, "recreate ONLY the vaultstore_test database")
	flag.Parse()
	ctx := context.Background()
	c, e := pgx.Connect(ctx, os.Getenv("DATABASE_URL"))
	if e != nil {
		log.Fatal(e)
	}
	defer c.Close(ctx)
	if *reset {
		if _, e = c.Exec(ctx, "DROP DATABASE IF EXISTS vaultstore_test WITH (FORCE)"); e != nil {
			log.Fatal(e)
		}
	}
	var exists bool
	if e = c.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname='vaultstore_test')").Scan(&exists); e != nil {
		log.Fatal(e)
	}
	if !exists {
		if _, e = c.Exec(ctx, "CREATE DATABASE vaultstore_test"); e != nil {
			log.Fatal(e)
		}
	}
}
