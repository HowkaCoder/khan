package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/HowkaCoder/khan/internal/config"
	"github.com/HowkaCoder/khan/internal/db"
	"github.com/HowkaCoder/khan/internal/ledger"
	"github.com/HowkaCoder/khan/internal/streams"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("svc", "settlement")
	err := run(log)
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Error("settlement завершился с ошибкой", "err", err)
		os.Exit(1)
	}
	log.Info("settlement остановлен")
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

	natsConn, js, err := streams.Connect(config.String("NATS_URL", config.DefaultNATS), "khan-settlement")
	if err != nil {
		return err
	}
	defer natsConn.Close()
	if err := streams.Ensure(ctx, js); err != nil {
		return err
	}

	settler := &ledger.Settler{DB: pool, JS: js, Log: log, Workers: config.Int("WORKERS", 4)}
	log.Info("settlement запущен", "workers", settler.Workers)
	runErr := settler.Run(ctx)

	if err := natsConn.Drain(); err != nil {
		log.Warn("не удалось аккуратно закрыть соединение с NATS", "err", err)
	}
	return runErr
}
