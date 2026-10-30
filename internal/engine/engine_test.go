package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/HowkaCoder/khan/internal/db"
	"github.com/HowkaCoder/khan/internal/events"
	"github.com/HowkaCoder/khan/internal/streams"
)

func TestCheckCoverage(t *testing.T) {
	cases := []struct {
		name                 string
		bookSeq, first, last uint64
		wantErr              error
	}{
		{"fresh stream", 0, 0, 0, nil},
		{"replay from start", 0, 1, 10, nil},
		{"replay tail", 5, 1, 10, nil},
		{"nothing to replay", 10, 1, 10, nil},
		{"purged after full apply", 10, 11, 10, nil},
		{"old commands deleted", 5, 7, 10, ErrReplayGap},
		{"no snapshot and stream trimmed", 0, 2, 10, ErrReplayGap},
		{"stream recreated", 10, 0, 0, ErrStreamBehind},
		{"stream shorter than snapshot", 10, 1, 8, ErrStreamBehind},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			err := checkCoverage(testCase.bookSeq, testCase.first, testCase.last)
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("err %v, want %v", err, testCase.wantErr)
			}
		})
	}
}

type testEnv struct {
	pool *pgxpool.Pool
	js   jetstream.JetStream
}

func newTestEnv(t *testing.T) testEnv {
	t.Helper()
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
	t.Cleanup(pool.Close)
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}

	conn, js, err := streams.Connect(natsURL, "khan-engine-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(conn.Close)
	if err := streams.Ensure(ctx, js); err != nil {
		t.Fatal(err)
	}
	return testEnv{pool: pool, js: js}
}

func (env testEnv) publishCommand(t *testing.T, symbol string, data []byte) uint64 {
	t.Helper()
	ack, err := env.js.Publish(context.Background(), events.OrdersSubject(symbol), data)
	if err != nil {
		t.Fatal(err)
	}
	return ack.Sequence
}

func commandJSON(t *testing.T, command events.Command) []byte {
	t.Helper()
	data, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func newTestEngine(env testEnv, symbol string) *Engine {
	return &Engine{
		Symbol:        symbol,
		DB:            env.pool,
		JS:            env.js,
		Log:           slog.New(slog.NewTextHandler(io.Discard, nil)),
		SnapshotEvery: 2,
	}
}

type runningEngine struct {
	engine *Engine
	cancel context.CancelFunc
	done   chan error
}

func start(engine *Engine) runningEngine {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- engine.Run(ctx) }()
	return runningEngine{engine: engine, cancel: cancel, done: done}
}

func (r runningEngine) stop(t *testing.T) {
	t.Helper()
	r.cancel()
	select {
	case err := <-r.done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("engine stopped with %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("engine did not stop after cancel")
	}
}

func waitForSeq(t *testing.T, engine *Engine, seq uint64) State {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		state := engine.State()
		if state.LastSeq >= seq {
			return state
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("engine did not reach seq %d, state %+v", seq, engine.State())
	return State{}
}

func waitForSnapshot(t *testing.T, env testEnv, symbol string, seq uint64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var saved bool
		err := env.pool.QueryRow(context.Background(),
			`SELECT EXISTS (SELECT 1 FROM book_snapshots WHERE symbol = $1 AND last_seq >= $2)`,
			symbol, seq).Scan(&saved)
		if err != nil {
			t.Fatal(err)
		}
		if saved {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("snapshot for seq %d was not saved", seq)
}

func readEvents(t *testing.T, env testEnv, symbol string) []events.Exec {
	t.Helper()
	ctx := context.Background()
	consumer, err := env.js.CreateConsumer(ctx, events.StreamTrades, jetstream.ConsumerConfig{
		FilterSubject: events.TradesSubject(symbol),
		AckPolicy:     jetstream.AckNonePolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		t.Fatal(err)
	}
	info, err := consumer.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}

	var result []events.Exec
	pending := int(info.NumPending)
	for len(result) < pending {
		batch, err := consumer.Fetch(pending-len(result), jetstream.FetchMaxWait(2*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		received := 0
		for message := range batch.Messages() {
			var event events.Exec
			if err := json.Unmarshal(message.Data(), &event); err != nil {
				t.Fatal(err)
			}
			result = append(result, event)
			received++
		}
		if received == 0 {
			break
		}
	}
	return result
}

func eventTypes(list []events.Exec) []string {
	result := make([]string, len(list))
	for i, event := range list {
		result[i] = event.Type + ":" + event.ID
	}
	return result
}

func TestEngineProcessesRestartsAndIgnoresDuplicates(t *testing.T) {
	env := newTestEnv(t)
	symbol := fmt.Sprintf("T%d-USD", time.Now().UnixNano())

	sellSeq := env.publishCommand(t, symbol, commandJSON(t, events.Command{Type: events.CmdNew, OrderID: "s1", Side: "sell", Price: 10000, Qty: 5}))
	buySeq := env.publishCommand(t, symbol, commandJSON(t, events.Command{Type: events.CmdNew, OrderID: "b1", Side: "buy", Price: 10100, Qty: 3}))
	env.publishCommand(t, symbol, []byte("{broken json"))
	env.publishCommand(t, symbol, commandJSON(t, events.Command{Type: events.CmdNew, OrderID: "b1", Side: "buy", Price: 10100, Qty: 3}))
	cancelSeq := env.publishCommand(t, symbol, commandJSON(t, events.Command{Type: events.CmdCancel, OrderID: "s1"}))
	lastSeq := env.publishCommand(t, symbol, commandJSON(t, events.Command{Type: events.CmdNew, OrderID: "x1", Side: "buy", Price: 0, Qty: 1}))

	first := start(newTestEngine(env, symbol))
	before := waitForSeq(t, first.engine, lastSeq)
	waitForSnapshot(t, env, symbol, lastSeq)

	second := newTestEngine(env, symbol)
	if err := second.Run(context.Background()); !errors.Is(err, db.ErrLockBusy) {
		t.Fatalf("second engine for the same symbol: err %v, want ErrLockBusy", err)
	}
	first.stop(t)

	if before.Orders != 0 || before.KnownOrders != 2 {
		t.Fatalf("state before restart %+v", before)
	}

	got := eventTypes(readEvents(t, env, symbol))
	want := []string{
		"accepted:" + events.AcceptedID(symbol, sellSeq),
		"accepted:" + events.AcceptedID(symbol, buySeq),
		fmt.Sprintf("trade:%s-%d-1", symbol, buySeq),
		"cancelled:" + events.CancelledID(symbol, cancelSeq),
		"rejected:" + events.RejectedID(symbol, lastSeq),
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("events\n got %v\nwant %v", got, want)
	}

	restarted := start(newTestEngine(env, symbol))
	after := waitForSeq(t, restarted.engine, lastSeq)
	restarted.stop(t)
	if after.Hash != before.Hash || after.LastSeq != before.LastSeq {
		t.Fatalf("state after restart %+v, before %+v", after, before)
	}
}

func TestEngineRefusesCorruptedSnapshot(t *testing.T) {
	env := newTestEnv(t)
	symbol := fmt.Sprintf("C%d-USD", time.Now().UnixNano())

	env.publishCommand(t, symbol, commandJSON(t, events.Command{Type: events.CmdNew, OrderID: "a", Side: "sell", Price: 100, Qty: 1}))
	lastSeq := env.publishCommand(t, symbol, commandJSON(t, events.Command{Type: events.CmdNew, OrderID: "b", Side: "sell", Price: 101, Qty: 1}))

	running := start(newTestEngine(env, symbol))
	waitForSeq(t, running.engine, lastSeq)
	waitForSnapshot(t, env, symbol, lastSeq)
	running.stop(t)

	_, err := env.pool.Exec(context.Background(),
		`UPDATE book_snapshots SET hash = 'corrupted' WHERE symbol = $1`, symbol)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = newTestEngine(env, symbol).Run(ctx)
	if !errors.Is(err, ErrSnapshotHash) {
		t.Fatalf("err %v, want ErrSnapshotHash", err)
	}
}
