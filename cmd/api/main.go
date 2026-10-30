package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/HowkaCoder/khan/internal/api"
	"github.com/HowkaCoder/khan/internal/config"
	"github.com/HowkaCoder/khan/internal/db"
	"github.com/HowkaCoder/khan/internal/outbox"
	"github.com/HowkaCoder/khan/internal/streams"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("svc", "api")
	if err := run(log); err != nil {
		log.Error("сервис завершился с ошибкой", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := db.Open(ctx, config.String("DATABASE_URL", config.DefaultDatabase))
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := db.Migrate(ctx, pool); err != nil {
		return err
	}

	natsConn, js, err := streams.Connect(config.String("NATS_URL", config.DefaultNATS), "khan-api")
	if err != nil {
		return err
	}
	defer natsConn.Close()
	if err := streams.Ensure(ctx, js); err != nil {
		return err
	}

	var background sync.WaitGroup
	if config.Bool("RELAY_ENABLED", true) {
		relay := &outbox.Relay{DB: pool, JS: js, Log: log.With("component", "relay")}
		background.Add(1)
		go func() {
			defer background.Done()
			relay.Run(ctx)
		}()
	}

	server := api.New(pool, config.List("SYMBOLS", config.DefaultSymbols), log)
	server.DepositsEnabled = config.Bool("DEPOSITS_ENABLED", true)

	httpServer := &http.Server{
		Addr:              config.String("HTTP_ADDR", ":8080"),
		Handler:           server.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serveErrors := make(chan error, 1)
	go func() {
		log.Info("HTTP сервер слушает", "addr", httpServer.Addr, "deposits_enabled", server.DepositsEnabled)
		serveErrors <- httpServer.ListenAndServe()
	}()

	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-serveErrors:
		stop()
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Warn("HTTP сервер не завершился вовремя", "err", err)
	}
	background.Wait()
	if err := natsConn.Drain(); err != nil {
		log.Warn("не удалось аккуратно закрыть соединение с NATS", "err", err)
	}

	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		return serveErr
	}
	log.Info("сервис остановлен")
	return nil
}
