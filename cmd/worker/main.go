package main

import (
	"log/slog"
	"os"
	"vaultstore/internal/app"
)

func main() {
	if e := app.Run("worker"); e != nil {
		slog.Error("service stopped", "error", e)
		os.Exit(1)
	}
}
