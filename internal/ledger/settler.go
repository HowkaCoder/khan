package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/HowkaCoder/khan/internal/events"
	"github.com/HowkaCoder/khan/internal/money"
	"github.com/HowkaCoder/khan/internal/streams"
)

const (
	ConsumerName   = "settlement"
	MaxDeliver     = 5
	defaultWorkers = 4
	fetchBatchSize = 100
	ackWait        = 30 * time.Second
	dlqSubject     = "dlq.settlement"
)

var errPermanent = errors.New("событие нельзя провести, повтор не поможет")

type Settler struct {
	DB      *pgxpool.Pool
	JS      jetstream.JetStream
	Log     *slog.Logger
	Workers int
}

func (s *Settler) Run(ctx context.Context) error {
	consumer, err := s.JS.CreateOrUpdateConsumer(ctx, events.StreamTrades, jetstream.ConsumerConfig{
		Durable:       ConsumerName,
		FilterSubject: "trades.>",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       ackWait,
		MaxDeliver:    MaxDeliver,
		MaxAckPending: 1024,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		return fmt.Errorf("не удалось создать consumer settlement: %w", err)
	}

	workers := s.Workers
	if workers <= 0 {
		workers = defaultWorkers
	}

	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.work(ctx, consumer)
		}()
	}
	wg.Wait()
	return ctx.Err()
}

func (s *Settler) work(ctx context.Context, consumer jetstream.Consumer) {
	for ctx.Err() == nil {
		batch, err := consumer.Fetch(fetchBatchSize, jetstream.FetchMaxWait(time.Second))
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.Log.Warn("не удалось получить события", "err", err)
			sleep(ctx, 500*time.Millisecond)
			continue
		}
		for message := range batch.Messages() {
			s.process(ctx, message)
		}
	}
}

func (s *Settler) process(ctx context.Context, message jetstream.Msg) {
	attempt := uint64(1)
	if metadata, err := message.Metadata(); err == nil {
		attempt = metadata.NumDelivered
	}

	err := s.handle(ctx, message.Data())
	if err == nil {
		if ackErr := message.Ack(); ackErr != nil {
			s.Log.Warn("не удалось подтвердить событие", "err", ackErr)
		}
		return
	}

	if ctx.Err() != nil {
		return
	}

	if errors.Is(err, errPermanent) || attempt >= MaxDeliver {
		s.moveToDLQ(ctx, message, err)
		return
	}

	s.Log.Warn("событие не проведено, будет повтор", "err", err, "attempt", attempt)
	if nakErr := message.NakWithDelay(time.Duration(attempt) * time.Second); nakErr != nil {
		s.Log.Warn("не удалось вернуть событие в очередь", "err", nakErr)
	}
}

func (s *Settler) moveToDLQ(ctx context.Context, message jetstream.Msg, cause error) {
	s.Log.Error("событие перемещено в DLQ", "err", cause)
	if err := streams.PublishDLQ(ctx, s.JS, dlqSubject, message.Data(), cause.Error()); err != nil {
		s.Log.Error("DLQ недоступен, событие будет доставлено повторно", "err", err)
		_ = message.Nak()
		return
	}
	if err := message.Term(); err != nil {
		s.Log.Warn("не удалось завершить доставку события", "err", err)
	}
}

func (s *Settler) handle(ctx context.Context, data []byte) error {
	var event events.Exec
	if err := json.Unmarshal(data, &event); err != nil {
		return fmt.Errorf("%w: не удалось разобрать JSON: %w", errPermanent, err)
	}
	if event.ID == "" {
		return fmt.Errorf("%w: у события нет id", errPermanent)
	}
	base, quote, err := money.SplitSymbol(event.Symbol)
	if err != nil {
		return fmt.Errorf("%w: %w", errPermanent, err)
	}

	return pgx.BeginFunc(ctx, s.DB, func(tx pgx.Tx) error {
		switch event.Type {
		case events.ExecTrade:
			return settleTrade(ctx, tx, event, base, quote)
		case events.ExecAccepted:
			return markAccepted(ctx, tx, event)
		case events.ExecRejected:
			return releaseRejected(ctx, tx, event, base, quote)
		case events.ExecCancelled:
			return releaseCancelled(ctx, tx, event, base, quote)
		default:
			return fmt.Errorf("%w: неизвестный тип события %q", errPermanent, event.Type)
		}
	})
}

type orderRow struct {
	id       string
	account  int64
	symbol   string
	side     string
	price    int64
	qty      int64
	filled   int64
	accepted bool
	rejected bool
}

func lockOrders(ctx context.Context, tx pgx.Tx, ids ...string) (map[string]orderRow, error) {
	rows, err := tx.Query(ctx, `
		SELECT id, account_id, symbol, side, price, qty, filled, accepted, rejected
		FROM orders
		WHERE id = ANY($1)
		ORDER BY id
		FOR UPDATE`, ids)
	if err != nil {
		return nil, fmt.Errorf("не удалось заблокировать заявки: %w", err)
	}
	defer rows.Close()

	result := make(map[string]orderRow, len(ids))
	for rows.Next() {
		var order orderRow
		err := rows.Scan(&order.id, &order.account, &order.symbol, &order.side,
			&order.price, &order.qty, &order.filled, &order.accepted, &order.rejected)
		if err != nil {
			return nil, err
		}
		result[order.id] = order
	}
	return result, rows.Err()
}

func lockOneOrder(ctx context.Context, tx pgx.Tx, event events.Exec) (orderRow, error) {
	orders, err := lockOrders(ctx, tx, event.OrderID)
	if err != nil {
		return orderRow{}, err
	}
	order, found := orders[event.OrderID]
	if !found {
		return orderRow{}, fmt.Errorf("%w: событие %s ссылается на неизвестную заявку %s", errPermanent, event.ID, event.OrderID)
	}
	if order.symbol != event.Symbol {
		return orderRow{}, fmt.Errorf("%w: заявка %s относится к паре %s, а событие к %s", errPermanent, order.id, order.symbol, event.Symbol)
	}
	return order, nil
}

type balanceKey struct {
	account int64
	asset   string
}

func lockBalances(ctx context.Context, tx pgx.Tx, keys ...balanceKey) error {
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].account != keys[j].account {
			return keys[i].account < keys[j].account
		}
		return keys[i].asset < keys[j].asset
	})

	accounts := make([]int64, len(keys))
	assets := make([]string, len(keys))
	for i, key := range keys {
		accounts[i] = key.account
		assets[i] = key.asset
	}

	_, err := tx.Exec(ctx, `
		INSERT INTO balances (account_id, asset)
		SELECT * FROM unnest($1::bigint[], $2::text[])
		ON CONFLICT DO NOTHING`, accounts, assets)
	if err != nil {
		return fmt.Errorf("не удалось создать строки балансов: %w", err)
	}

	_, err = tx.Exec(ctx, `
		SELECT 1
		FROM balances b
		JOIN unnest($1::bigint[], $2::text[]) AS k(account_id, asset)
		  ON b.account_id = k.account_id AND b.asset = k.asset
		ORDER BY b.account_id, b.asset
		FOR UPDATE OF b`, accounts, assets)
	if err != nil {
		return fmt.Errorf("не удалось заблокировать балансы: %w", err)
	}
	return nil
}

func changeBalance(ctx context.Context, tx pgx.Tx, account int64, asset string, availableDelta, heldDelta int64) error {
	_, err := tx.Exec(ctx, `
		UPDATE balances
		SET available = available + $3, held = held + $4
		WHERE account_id = $1 AND asset = $2`, account, asset, availableDelta, heldDelta)
	if err != nil {
		return fmt.Errorf("не удалось изменить баланс счёта %d по %s: %w", account, asset, err)
	}
	return nil
}

type tradeParties struct {
	buy  orderRow
	sell orderRow
}

func findTradeParties(ctx context.Context, tx pgx.Tx, event events.Exec) (tradeParties, error) {
	orders, err := lockOrders(ctx, tx, event.MakerOrderID, event.TakerOrderID)
	if err != nil {
		return tradeParties{}, err
	}
	maker, makerFound := orders[event.MakerOrderID]
	taker, takerFound := orders[event.TakerOrderID]
	if !makerFound || !takerFound {
		return tradeParties{}, fmt.Errorf("%w: сделка %s ссылается на неизвестную заявку", errPermanent, event.ID)
	}

	var parties tradeParties
	switch event.TakerSide {
	case "buy":
		parties = tradeParties{buy: taker, sell: maker}
	case "sell":
		parties = tradeParties{buy: maker, sell: taker}
	default:
		return tradeParties{}, fmt.Errorf("%w: у сделки %s неизвестная сторона taker %q", errPermanent, event.ID, event.TakerSide)
	}

	if err := checkTradeParties(event, parties); err != nil {
		return tradeParties{}, err
	}
	return parties, nil
}

func checkTradeParties(event events.Exec, parties tradeParties) error {
	buy, sell := parties.buy, parties.sell
	switch {
	case buy.side != "buy" || sell.side != "sell":
		return fmt.Errorf("%w: стороны заявок в сделке %s не совпадают", errPermanent, event.ID)
	case buy.symbol != event.Symbol || sell.symbol != event.Symbol:
		return fmt.Errorf("%w: заявки сделки %s относятся к другой паре", errPermanent, event.ID)
	case event.Qty <= 0 || event.Price <= 0:
		return fmt.Errorf("%w: у сделки %s неположительные цена или количество", errPermanent, event.ID)
	case event.Price > buy.price || event.Price < sell.price:
		return fmt.Errorf("%w: цена сделки %s вне лимитов заявок", errPermanent, event.ID)
	case buy.rejected || sell.rejected:
		return fmt.Errorf("%w: сделка %s ссылается на отклонённую заявку", errPermanent, event.ID)
	}
	return nil
}

func settleTrade(ctx context.Context, tx pgx.Tx, event events.Exec, base, quote string) error {
	parties, err := findTradeParties(ctx, tx, event)
	if err != nil {
		return err
	}
	buy, sell := parties.buy, parties.sell

	tag, err := tx.Exec(ctx, `
		INSERT INTO trades (trade_id, symbol, seq, buy_order, sell_order, price, qty)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (trade_id) DO NOTHING`,
		event.ID, event.Symbol, event.Seq, buy.id, sell.id, event.Price, event.Qty)
	if err != nil {
		return fmt.Errorf("не удалось записать сделку %s: %w", event.ID, err)
	}
	if tag.RowsAffected() == 0 {
		return nil
	}

	cost, err := money.Mul(event.Price, event.Qty)
	if err != nil {
		return fmt.Errorf("%w: стоимость сделки %s: %w", errPermanent, event.ID, err)
	}
	reserved, err := money.Mul(buy.price, event.Qty)
	if err != nil {
		return fmt.Errorf("%w: резерв покупателя в сделке %s: %w", errPermanent, event.ID, err)
	}
	priceImprovement := reserved - cost

	err = lockBalances(ctx, tx,
		balanceKey{buy.account, quote},
		balanceKey{buy.account, base},
		balanceKey{sell.account, quote},
		balanceKey{sell.account, base},
	)
	if err != nil {
		return err
	}

	if err := changeBalance(ctx, tx, buy.account, quote, priceImprovement, -reserved); err != nil {
		return err
	}
	if err := changeBalance(ctx, tx, buy.account, base, event.Qty, 0); err != nil {
		return err
	}
	if err := changeBalance(ctx, tx, sell.account, base, 0, -event.Qty); err != nil {
		return err
	}
	if err := changeBalance(ctx, tx, sell.account, quote, cost, 0); err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO ledger_entries (ref, account_id, asset, amount) VALUES
			($1, $2, $4, -$6::bigint),
			($1, $3, $4, $6::bigint),
			($1, $3, $5, -$7::bigint),
			($1, $2, $5, $7::bigint)`,
		event.ID, buy.account, sell.account, quote, base, cost, event.Qty)
	if err != nil {
		return fmt.Errorf("не удалось записать проводки сделки %s: %w", event.ID, err)
	}

	_, err = tx.Exec(ctx, `UPDATE orders SET filled = filled + $2 WHERE id = ANY($1)`,
		[]string{buy.id, sell.id}, event.Qty)
	if err != nil {
		return fmt.Errorf("не удалось обновить исполнение заявок сделки %s: %w", event.ID, err)
	}
	return nil
}

func markEventProcessed(ctx context.Context, tx pgx.Tx, eventID string) (bool, error) {
	tag, err := tx.Exec(ctx, `
		INSERT INTO processed_events (event_id) VALUES ($1)
		ON CONFLICT DO NOTHING`, eventID)
	if err != nil {
		return false, fmt.Errorf("не удалось отметить событие %s: %w", eventID, err)
	}
	return tag.RowsAffected() == 1, nil
}

func markAccepted(ctx context.Context, tx pgx.Tx, event events.Exec) error {
	isNew, err := markEventProcessed(ctx, tx, event.ID)
	if err != nil || !isNew {
		return err
	}
	order, err := lockOneOrder(ctx, tx, event)
	if err != nil {
		return err
	}
	if order.rejected {
		return fmt.Errorf("%w: заявка %s уже отклонена, принять её нельзя", errPermanent, order.id)
	}
	_, err = tx.Exec(ctx, `UPDATE orders SET accepted = true WHERE id = $1`, order.id)
	return err
}

func releaseRejected(ctx context.Context, tx pgx.Tx, event events.Exec, base, quote string) error {
	isNew, err := markEventProcessed(ctx, tx, event.ID)
	if err != nil || !isNew {
		return err
	}
	order, err := lockOneOrder(ctx, tx, event)
	if err != nil {
		return err
	}
	if order.accepted || order.filled > 0 {
		return fmt.Errorf("%w: заявка %s уже в стакане, отклонить её нельзя", errPermanent, order.id)
	}
	if err := releaseReserve(ctx, tx, order, order.qty, base, quote); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE orders SET rejected = true WHERE id = $1`, order.id)
	return err
}

func releaseCancelled(ctx context.Context, tx pgx.Tx, event events.Exec, base, quote string) error {
	isNew, err := markEventProcessed(ctx, tx, event.ID)
	if err != nil || !isNew {
		return err
	}
	order, err := lockOneOrder(ctx, tx, event)
	if err != nil {
		return err
	}
	if event.Remaining <= 0 || event.Remaining > order.qty {
		return fmt.Errorf("%w: в отмене %s неверный остаток %d", errPermanent, event.ID, event.Remaining)
	}
	if err := releaseReserve(ctx, tx, order, event.Remaining, base, quote); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE orders SET cancelled_qty = $2 WHERE id = $1`, order.id, event.Remaining)
	return err
}

func releaseReserve(ctx context.Context, tx pgx.Tx, order orderRow, quantity int64, base, quote string) error {
	asset := base
	amount := quantity
	if order.side == "buy" {
		asset = quote
		var err error
		amount, err = money.Mul(order.price, quantity)
		if err != nil {
			return fmt.Errorf("%w: резерв заявки %s: %w", errPermanent, order.id, err)
		}
	}
	if amount == 0 {
		return nil
	}
	if err := lockBalances(ctx, tx, balanceKey{order.account, asset}); err != nil {
		return err
	}
	return changeBalance(ctx, tx, order.account, asset, amount, -amount)
}

func sleep(ctx context.Context, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
