package outbox

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/HowkaCoder/khan/internal/db"
	"github.com/HowkaCoder/khan/internal/events"
	"github.com/HowkaCoder/khan/internal/streams"
)

func TestRelayPublishesInOrderOnceAndLocks(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	natsURL := os.Getenv("TEST_NATS_URL")
	if databaseURL == "" || natsURL == "" {
		t.Skip("нужны TEST_DATABASE_URL и TEST_NATS_URL")
	}
	ctx := context.Background()

	pool, err := db.Open(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	conn, js, err := streams.Connect(natsURL, "khan-relay-test")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := streams.Ensure(ctx, js); err != nil {
		t.Fatal(err)
	}

	lock, err := db.TryLock(ctx, pool, LockName)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.TryLock(ctx, pool, LockName); !errors.Is(err, db.ErrLockBusy) {
		t.Fatalf("second lock: err %v, want ErrLockBusy", err)
	}
	if err := lock.Check(ctx); err != nil {
		t.Fatal(err)
	}

	if _, err := pool.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE published_at IS NULL`); err != nil {
		t.Fatal(err)
	}

	symbol := fmt.Sprintf("R%d-USD", time.Now().UnixNano())
	subject := events.OrdersSubject(symbol)
	for i := 1; i <= 3; i++ {
		_, err := pool.Exec(ctx, `INSERT INTO outbox (subject, msg_id, payload) VALUES ($1, $2, $3)`,
			subject, fmt.Sprintf("%s-msg-%d", symbol, i), []byte(fmt.Sprintf(`{"n":%d}`, i)))
		if err != nil {
			t.Fatal(err)
		}
	}

	relay := &Relay{DB: pool, JS: js, Log: slog.New(slog.NewTextHandler(io.Discard, nil)), BatchSize: 10}
	published, err := relay.PublishBatch(ctx)
	if err != nil || published != 3 {
		t.Fatalf("published %d, err %v", published, err)
	}

	_, err = pool.Exec(ctx, `UPDATE outbox SET published_at = NULL WHERE subject = $1`, subject)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := relay.PublishBatch(ctx); err != nil {
		t.Fatal(err)
	}
	lock.Release()

	consumer, err := js.CreateConsumer(ctx, events.StreamOrders, jetstream.ConsumerConfig{
		FilterSubject: subject,
		AckPolicy:     jetstream.AckNonePolicy,
	})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := consumer.Fetch(10, jetstream.FetchMaxWait(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	var payloads []string
	for message := range batch.Messages() {
		payloads = append(payloads, string(message.Data()))
	}
	want := `[{"n":1} {"n":2} {"n":3}]`
	if fmt.Sprint(payloads) != want {
		t.Fatalf("stream has %v, want %s (republish inside the dedup window must be dropped)", payloads, want)
	}
}
