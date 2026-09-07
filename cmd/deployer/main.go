package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/mistriq/deployer/internal/app"
	"os"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "migrate-sqlite" {
		flags := flag.NewFlagSet("migrate-sqlite", flag.ExitOnError)
		source := flags.String("source", "deployer.db", "source SQLite database")
		_ = flags.Parse(os.Args[2:])
		if err := app.ImportSQLite(context.Background(), *source, os.Getenv("DEPLOYER_DATABASE_URL"), os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "deployer migrate-sqlite:", err)
			os.Exit(1)
		}
		return
	}
	app.Run()
}
