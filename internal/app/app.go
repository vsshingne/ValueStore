package app

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	"vaultstore/internal/api"
	"vaultstore/internal/config"
	"vaultstore/internal/engine"
	"vaultstore/internal/jobs"
	"vaultstore/internal/metadata"
	"vaultstore/internal/metrics"
	"vaultstore/internal/storage"
)

func Run(role string) error {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	c, err := config.Load()
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	var repo *metadata.Repository
	for n := 0; n < 30; n++ {
		repo, err = metadata.Open(ctx, c.DatabaseURL, c.Topic)
		if err == nil {
			break
		}
		slog.Warn("database startup retry", "attempt", n+1)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	if err != nil {
		return err
	}
	defer repo.Pool.Close()
	if err = repo.Migrate(ctx); err != nil {
		return err
	}
	store, err := storage.New(c)
	if err != nil {
		return err
	}
	for n := 0; n < 30; n++ {
		err = store.Ensure(ctx)
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	if err != nil {
		return err
	}
	for _, p := range []string{c.BackupRoot, c.RestoreRoot} {
		if err = os.MkdirAll(p, 0750); err != nil {
			return err
		}
	}
	e := &engine.Engine{Repo: repo, Store: store, Config: c}
	metrics.Register(repo)
	if role == "worker" || role == "scheduler" {
		for attempt := 0; attempt < 30; attempt++ {
			err = jobs.EnsureTopics(ctx, c.Brokers, c.Topic)
			if err == nil {
				break
			}
			slog.Warn("Kafka startup retry", "error", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
			}
		}
		if err != nil {
			return err
		}
	}
	server := &http.Server{Addr: c.Listen, Handler: (&api.API{Engine: e}).Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 35 * time.Second, IdleTimeout: 60 * time.Second}
	errs := make(chan error, 2)
	go func() { errs <- server.ListenAndServe() }()
	switch role {
	case "worker":
		go func() { errs <- jobs.Workers(ctx, e) }()
	case "scheduler":
		go func() { errs <- jobs.Scheduler(ctx, e) }()
	}
	slog.Info("service started", "role", role, "listen", c.Listen)
	select {
	case <-ctx.Done():
	case err = <-errs:
		cancel()
	}
	shutdown, cancelShutdown := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelShutdown()
	if er := server.Shutdown(shutdown); er != nil {
		return er
	}
	if errors.Is(err, http.ErrServerClosed) || errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
