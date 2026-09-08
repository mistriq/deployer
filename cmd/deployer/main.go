package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/mistriq/deployer/internal/app"
)

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "mcp":
			if err := app.RunMCP(context.Background(), os.Stdin, os.Stdout); err != nil {
				fmt.Fprintln(os.Stderr, "deployer mcp:", err)
				os.Exit(1)
			}
			return
		case "migrate-sqlite":
			flags := flag.NewFlagSet("migrate-sqlite", flag.ExitOnError)
			source := flags.String("source", "deployer.db", "source SQLite database")
			_ = flags.Parse(os.Args[2:])
			if err := app.ImportSQLite(context.Background(), *source, os.Getenv("DEPLOYER_DATABASE_URL"), os.Stdout); err != nil {
				fmt.Fprintln(os.Stderr, "deployer migrate-sqlite:", err)
				os.Exit(1)
			}
			return
		}
	}
	app.Run()
}
