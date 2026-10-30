package db

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/HowkaCoder/khan/migrations"
)

const migrationLockKey = 7_349_120

var ErrLockBusy = errors.New("блокировка уже занята другим процессом")

func Open(ctx context.Context, url string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("не удалось создать пул PostgreSQL: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("PostgreSQL не отвечает: %w", err)
	}
	return pool, nil
}

func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("не удалось получить соединение для миграций: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationLockKey); err != nil {
		return fmt.Errorf("не удалось взять блокировку миграций: %w", err)
	}
	defer unlockMigrations(conn)

	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		name text PRIMARY KEY,
		applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return fmt.Errorf("не удалось создать schema_migrations: %w", err)
	}

	names, err := fs.Glob(migrations.FS, "*.sql")
	if err != nil {
		return err
	}
	sort.Strings(names)

	for _, name := range names {
		if err := applyMigration(ctx, conn, name); err != nil {
			return err
		}
	}
	return nil
}

func unlockMigrations(conn *pgxpool.Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, migrationLockKey); err != nil {
		conn.Conn().Close(ctx)
	}
}

func applyMigration(ctx context.Context, conn *pgxpool.Conn, name string) error {
	var alreadyApplied bool
	err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE name = $1)`, name).Scan(&alreadyApplied)
	if err != nil {
		return fmt.Errorf("не удалось проверить миграцию %s: %w", name, err)
	}
	if alreadyApplied {
		return nil
	}

	script, err := migrations.FS.ReadFile(name)
	if err != nil {
		return err
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())

	if _, err := tx.Exec(ctx, string(script)); err != nil {
		return fmt.Errorf("миграция %s не применилась: %w", name, err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (name) VALUES ($1)`, name); err != nil {
		return fmt.Errorf("не удалось записать миграцию %s: %w", name, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("не удалось зафиксировать миграцию %s: %w", name, err)
	}
	return nil
}

type Lock struct {
	name string
	conn *pgxpool.Conn
}

func TryLock(ctx context.Context, pool *pgxpool.Pool, name string) (*Lock, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("не удалось получить соединение для блокировки %s: %w", name, err)
	}

	var acquired bool
	err = conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtextextended($1, 0))`, name).Scan(&acquired)
	if err != nil {
		conn.Release()
		return nil, fmt.Errorf("не удалось взять блокировку %s: %w", name, err)
	}
	if !acquired {
		conn.Release()
		return nil, fmt.Errorf("%w: %s", ErrLockBusy, name)
	}
	return &Lock{name: name, conn: conn}, nil
}

func (l *Lock) Check(ctx context.Context) error {
	if _, err := l.conn.Exec(ctx, `SELECT 1`); err != nil {
		return fmt.Errorf("соединение с блокировкой %s потеряно: %w", l.name, err)
	}
	return nil
}

func (l *Lock) Release() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := l.conn.Exec(ctx, `SELECT pg_advisory_unlock(hashtextextended($1, 0))`, l.name); err != nil {
		l.conn.Conn().Close(ctx)
	}
	l.conn.Release()
}
