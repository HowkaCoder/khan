package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/HowkaCoder/khan/internal/db"
)

const (
	LockName          = "khan-outbox-relay"
	defaultBatchSize  = 100
	defaultIdlePause  = 20 * time.Millisecond
	errorPause        = time.Second
	standbyPause      = 5 * time.Second
	lockCheckInterval = 5 * time.Second
)

type Relay struct {
	DB        *pgxpool.Pool
	JS        jetstream.JetStream
	Log       *slog.Logger
	BatchSize int
	IdlePause time.Duration
}

type outboxRow struct {
	ID      int64
	Subject string
	MsgID   string
	Payload []byte
}

func (r *Relay) Run(ctx context.Context) {
	if r.BatchSize == 0 {
		r.BatchSize = defaultBatchSize
	}
	if r.IdlePause == 0 {
		r.IdlePause = defaultIdlePause
	}

	for ctx.Err() == nil {
		lock, err := db.TryLock(ctx, r.DB, LockName)
		if err != nil {
			if !errors.Is(err, db.ErrLockBusy) && ctx.Err() == nil {
				r.Log.Warn("relay не смог взять блокировку", "err", err)
			}
			sleep(ctx, standbyPause)
			continue
		}

		r.Log.Info("relay стал активным")
		err = r.publishWhileLocked(ctx, lock)
		lock.Release()
		if err != nil && ctx.Err() == nil {
			r.Log.Error("relay остановлен, попробуем снова", "err", err)
			sleep(ctx, errorPause)
		}
	}
}

func (r *Relay) publishWhileLocked(ctx context.Context, lock *db.Lock) error {
	lastLockCheck := time.Now()
	for ctx.Err() == nil {
		if time.Since(lastLockCheck) >= lockCheckInterval {
			if err := lock.Check(ctx); err != nil {
				return err
			}
			lastLockCheck = time.Now()
		}

		published, err := r.PublishBatch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			r.Log.Error("не удалось опубликовать пачку outbox", "err", err)
			sleep(ctx, errorPause)
			continue
		}
		if published == 0 {
			sleep(ctx, r.IdlePause)
		}
	}
	return nil
}

func (r *Relay) PublishBatch(ctx context.Context) (int, error) {
	published := 0
	err := pgx.BeginFunc(ctx, r.DB, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT id, subject, msg_id, payload
			FROM outbox
			WHERE published_at IS NULL
			ORDER BY id
			LIMIT $1
			FOR UPDATE`, r.BatchSize)
		if err != nil {
			return fmt.Errorf("не удалось прочитать outbox: %w", err)
		}
		batch, err := pgx.CollectRows(rows, pgx.RowToStructByPos[outboxRow])
		if err != nil {
			return fmt.Errorf("не удалось прочитать строки outbox: %w", err)
		}

		publishedIDs := r.publishInOrder(ctx, batch)
		if len(publishedIDs) == 0 {
			return nil
		}

		_, err = tx.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, publishedIDs)
		if err != nil {
			return fmt.Errorf("не удалось отметить опубликованные строки: %w", err)
		}
		published = len(publishedIDs)
		return nil
	})
	return published, err
}

func (r *Relay) publishInOrder(ctx context.Context, batch []outboxRow) []int64 {
	publishedIDs := make([]int64, 0, len(batch))
	for _, row := range batch {
		_, err := r.JS.Publish(ctx, row.Subject, row.Payload, jetstream.WithMsgID(row.MsgID))
		if err != nil {
			r.Log.Warn("публикация команды не удалась, остаток пачки отправим позже",
				"msg_id", row.MsgID, "err", err)
			break
		}
		publishedIDs = append(publishedIDs, row.ID)
	}
	return publishedIDs
}

func sleep(ctx context.Context, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
