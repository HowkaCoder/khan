package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/HowkaCoder/khan/internal/config"
	"github.com/HowkaCoder/khan/internal/db"
	"github.com/HowkaCoder/khan/internal/engine"
	"github.com/HowkaCoder/khan/internal/streams"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("svc", "engine")
	err := run(log)
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Error("движок завершился с ошибкой", "err", err)
		os.Exit(1)
	}
	log.Info("движок остановлен")
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

	natsConn, js, err := streams.Connect(config.String("NATS_URL", config.DefaultNATS), "khan-engine")
	if err != nil {
		return err
	}
	defer natsConn.Close()
	if err := streams.Ensure(ctx, js); err != nil {
		return err
	}

	matchingEngine := &engine.Engine{
		Symbol:        config.String("ENGINE_SYMBOL", config.DefaultSymbols),
		DB:            pool,
		JS:            js,
		Log:           log,
		SnapshotEvery: uint64(max(0, config.Int("SNAPSHOT_EVERY", 1000))),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /state", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(matchingEngine.State())
	})
	httpServer := &http.Server{
		Addr:              config.String("HTTP_ADDR", ":8081"),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      10 * time.Second,
	}
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("HTTP сервер движка остановился", "err", err)
		}
	}()

	runErr := matchingEngine.Run(ctx)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Warn("HTTP сервер движка не завершился вовремя", "err", err)
	}
	if err := natsConn.Drain(); err != nil {
		log.Warn("не удалось аккуратно закрыть соединение с NATS", "err", err)
	}
	return runErr
}
