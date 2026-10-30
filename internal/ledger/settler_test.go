package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/HowkaCoder/khan/internal/db"
	"github.com/HowkaCoder/khan/internal/events"
)

func testDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("нужен TEST_DATABASE_URL")
	}
	ctx := context.Background()
	pool, err := db.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func setup(t *testing.T, pool *pgxpool.Pool, buyer, seller int64, buyPrice, qty int64) (buyID, sellID string) {
	t.Helper()
	ctx := context.Background()
	n := time.Now().UnixNano()
	buyID, sellID = fmt.Sprintf("tb_%d", n), fmt.Sprintf("ts_%d", n)
	stmts := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO balances (account_id, asset, available, held) VALUES ($1, 'USD', 0, $2)
		  ON CONFLICT (account_id, asset) DO UPDATE SET held = balances.held + EXCLUDED.held`, []any{buyer, buyPrice * qty}},
		{`INSERT INTO balances (account_id, asset, available, held) VALUES ($1, 'ACME', 0, $2)
		  ON CONFLICT (account_id, asset) DO UPDATE SET held = balances.held + EXCLUDED.held`, []any{seller, qty}},
		{`INSERT INTO orders (id, account_id, symbol, side, price, qty) VALUES ($1, $2, 'ACME-USD', 'buy', $3, $4)`, []any{buyID, buyer, buyPrice, qty}},
		{`INSERT INTO orders (id, account_id, symbol, side, price, qty) VALUES ($1, $2, 'ACME-USD', 'sell', 10000, $3)`, []any{sellID, seller, qty}},
	}
	for _, s := range stmts {
		if _, err := pool.Exec(ctx, s.sql, s.args...); err != nil {
			t.Fatal(err)
		}
	}
	return buyID, sellID
}

func balance(t *testing.T, pool *pgxpool.Pool, acc int64, asset string) (avail, held int64) {
	t.Helper()
	err := pool.QueryRow(context.Background(),
		`SELECT available, held FROM balances WHERE account_id = $1 AND asset = $2`, acc, asset).Scan(&avail, &held)
	if err != nil {
		t.Fatal(err)
	}
	return
}

func TestTradeSettlementIsIdempotent(t *testing.T) {
	pool := testDB(t)
	s := &Settler{DB: pool}
	buyer, seller := time.Now().UnixNano()%1e9+1000, time.Now().UnixNano()%1e9+2000
	buyID, sellID := setup(t, pool, buyer, seller, 10100, 3)

	ev := events.Exec{
		Type: events.ExecTrade, ID: "test-" + buyID, Symbol: "ACME-USD", Seq: 1,
		Price: 10000, Qty: 3, MakerOrderID: sellID, TakerOrderID: buyID, TakerSide: "buy",
	}
	data, _ := json.Marshal(ev)

	for i := 0; i < 3; i++ {
		if err := s.handle(context.Background(), data); err != nil {
			t.Fatalf("attempt %d: %v", i, err)
		}
	}

	var entries int
	_ = pool.QueryRow(context.Background(), `SELECT count(*) FROM ledger_entries WHERE ref = $1`, ev.ID).Scan(&entries)
	if entries != 4 {
		t.Fatalf("want 4 ledger entries, got %d", entries)
	}
	if a, h := balance(t, pool, buyer, "USD"); a != 300 || h != 0 {
		t.Fatalf("buyer USD available=%d held=%d, want 300 (price improvement) and 0", a, h)
	}
	if a, _ := balance(t, pool, buyer, "ACME"); a != 3 {
		t.Fatalf("buyer ACME = %d", a)
	}
	if a, h := balance(t, pool, seller, "USD"); a != 30000 || h != 0 {
		t.Fatalf("seller USD available=%d held=%d", a, h)
	}
	if _, h := balance(t, pool, seller, "ACME"); h != 0 {
		t.Fatalf("seller ACME held = %d", h)
	}
}

func TestConcurrentOppositeTradesNoDeadlock(t *testing.T) {
	pool := testDB(t)
	s := &Settler{DB: pool}
	a, b := time.Now().UnixNano()%1e9+3000, time.Now().UnixNano()%1e9+4000

	var msgs [][]byte
	for i := 0; i < 40; i++ {
		buyer, seller := a, b
		if i%2 == 1 {
			buyer, seller = b, a
		}
		buyID, sellID := setup(t, pool, buyer, seller, 10000, 1)
		ev := events.Exec{Type: events.ExecTrade, ID: fmt.Sprintf("dl-%s", buyID), Symbol: "ACME-USD", Seq: uint64(i + 1),
			Price: 10000, Qty: 1, MakerOrderID: sellID, TakerOrderID: buyID, TakerSide: "buy"}
		data, _ := json.Marshal(ev)
		msgs = append(msgs, data)
	}

	var wg sync.WaitGroup
	errs := make(chan error, len(msgs))
	for _, m := range msgs {
		wg.Add(1)
		go func(m []byte) {
			defer wg.Done()
			errs <- s.handle(context.Background(), m)
		}(m)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("settlement failed: %v", err)
		}
	}
}

func uniqueAccounts() (int64, int64) {
	base := time.Now().UnixNano()%1e9 + 10_000
	return base, base + 1
}

func encodeEvent(t *testing.T, event events.Exec) []byte {
	t.Helper()
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func orderFlags(t *testing.T, pool *pgxpool.Pool, id string) (status string, cancelled int64) {
	t.Helper()
	err := pool.QueryRow(context.Background(),
		`SELECT status, cancelled_qty FROM orders WHERE id = $1`, id).Scan(&status, &cancelled)
	if err != nil {
		t.Fatal(err)
	}
	return status, cancelled
}

func TestCancelReleasesOnlyRemainingReserve(t *testing.T) {
	pool := testDB(t)
	s := &Settler{DB: pool}
	ctx := context.Background()
	buyer, seller := uniqueAccounts()
	buyID, sellID := setup(t, pool, buyer, seller, 10100, 5)

	trade := events.Exec{Type: events.ExecTrade, ID: "pc-" + buyID, Symbol: "ACME-USD", Seq: 2,
		Price: 10000, Qty: 2, MakerOrderID: sellID, TakerOrderID: buyID, TakerSide: "buy"}
	cancel := events.Exec{Type: events.ExecCancelled, ID: "cx-" + buyID, Symbol: "ACME-USD", Seq: 3,
		OrderID: buyID, Remaining: 3}

	for _, event := range []events.Exec{cancel, trade, cancel} {
		if err := s.handle(ctx, encodeEvent(t, event)); err != nil {
			t.Fatal(err)
		}
	}

	available, held := balance(t, pool, buyer, "USD")
	if held != 0 || available != 3*10100+2*100 {
		t.Fatalf("buyer USD available=%d held=%d", available, held)
	}
	status, cancelled := orderFlags(t, pool, buyID)
	if status != "cancelled" || cancelled != 3 {
		t.Fatalf("status %s cancelled %d", status, cancelled)
	}
}

func TestRejectReleasesWholeReserveOnce(t *testing.T) {
	pool := testDB(t)
	s := &Settler{DB: pool}
	ctx := context.Background()
	buyer, seller := uniqueAccounts()
	buyID, _ := setup(t, pool, buyer, seller, 10000, 4)

	reject := events.Exec{Type: events.ExecRejected, ID: "rj-" + buyID, Symbol: "ACME-USD", Seq: 1, OrderID: buyID}
	for i := 0; i < 2; i++ {
		if err := s.handle(ctx, encodeEvent(t, reject)); err != nil {
			t.Fatal(err)
		}
	}
	if available, held := balance(t, pool, buyer, "USD"); available != 40000 || held != 0 {
		t.Fatalf("buyer USD available=%d held=%d", available, held)
	}
	if status, _ := orderFlags(t, pool, buyID); status != "rejected" {
		t.Fatalf("status %s", status)
	}
}

func TestContradictoryEventsArePermanentErrors(t *testing.T) {
	pool := testDB(t)
	s := &Settler{DB: pool}
	ctx := context.Background()
	buyer, seller := uniqueAccounts()
	buyID, sellID := setup(t, pool, buyer, seller, 10000, 2)

	accept := events.Exec{Type: events.ExecAccepted, ID: "ac-" + buyID, Symbol: "ACME-USD", Seq: 1, OrderID: buyID}
	if err := s.handle(ctx, encodeEvent(t, accept)); err != nil {
		t.Fatal(err)
	}

	cases := map[string]events.Exec{
		"reject accepted order": {Type: events.ExecRejected, ID: "rj-" + buyID, Symbol: "ACME-USD", Seq: 2, OrderID: buyID},
		"price above buy limit": {Type: events.ExecTrade, ID: "tp-" + buyID, Symbol: "ACME-USD", Seq: 3,
			Price: 10001, Qty: 1, MakerOrderID: sellID, TakerOrderID: buyID, TakerSide: "buy"},
		"wrong taker side": {Type: events.ExecTrade, ID: "ts-" + buyID, Symbol: "ACME-USD", Seq: 4,
			Price: 10000, Qty: 1, MakerOrderID: buyID, TakerOrderID: sellID, TakerSide: "buy"},
		"other symbol":    {Type: events.ExecAccepted, ID: "os-" + buyID, Symbol: "OTHER-USD", Seq: 5, OrderID: buyID},
		"unknown order":   {Type: events.ExecAccepted, ID: "uo-" + buyID, Symbol: "ACME-USD", Seq: 6, OrderID: "missing"},
		"cancel too much": {Type: events.ExecCancelled, ID: "ct-" + buyID, Symbol: "ACME-USD", Seq: 7, OrderID: buyID, Remaining: 3},
		"unknown type":    {Type: "mystery", ID: "mt-" + buyID, Symbol: "ACME-USD"},
		"broken symbol":   {Type: events.ExecAccepted, ID: "bs-" + buyID, Symbol: "ACMEUSD", OrderID: buyID},
	}
	for name, event := range cases {
		t.Run(name, func(t *testing.T) {
			if err := s.handle(ctx, encodeEvent(t, event)); !errors.Is(err, errPermanent) {
				t.Fatalf("err %v, want errPermanent", err)
			}
		})
	}
	if err := s.handle(ctx, []byte("not json")); !errors.Is(err, errPermanent) {
		t.Fatalf("broken json: err %v", err)
	}

	var processed int
	err := pool.QueryRow(ctx, `SELECT count(*) FROM processed_events WHERE event_id = ANY($1)`,
		[]string{"rj-" + buyID, "ct-" + buyID}).Scan(&processed)
	if err != nil {
		t.Fatal(err)
	}
	if processed != 0 {
		t.Fatalf("failed events must roll back their processed mark, found %d", processed)
	}
	if available, held := balance(t, pool, buyer, "USD"); available != 0 || held != 20000 {
		t.Fatalf("reserve changed by rejected events: available=%d held=%d", available, held)
	}
}
