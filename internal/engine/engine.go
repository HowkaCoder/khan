package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/HowkaCoder/khan/internal/book"
	"github.com/HowkaCoder/khan/internal/db"
	"github.com/HowkaCoder/khan/internal/events"
	"github.com/HowkaCoder/khan/internal/streams"
)

const (
	defaultSnapshotEvery = 1000
	fetchBatchSize       = 500
	fetchWait            = 250 * time.Millisecond
	publishAckTimeout    = 10 * time.Second
	lockCheckInterval    = 5 * time.Second
	snapshotsToKeep      = 3
	stateDepth           = 10
)

var (
	ErrReplayGap           = errors.New("в стриме ORDERS нет части команд, нужных для восстановления стакана")
	ErrStreamBehind        = errors.New("стрим ORDERS короче, чем состояние из снапшота")
	ErrSnapshotHash        = errors.New("хэш восстановленного стакана не совпадает с сохранённым")
	ErrSnapshotSymbol      = errors.New("снапшот принадлежит другой торговой паре")
	ErrPublishNotConfirmed = errors.New("JetStream не подтвердил публикацию события")
)

type Engine struct {
	Symbol        string
	DB            *pgxpool.Pool
	JS            jetstream.JetStream
	Log           *slog.Logger
	SnapshotEvery uint64

	mu                    sync.RWMutex
	book                  *book.Book
	commandsSinceSnapshot uint64
	pending               []jetstream.PubAckFuture
}

type State struct {
	Symbol      string       `json:"symbol"`
	LastSeq     uint64       `json:"last_seq"`
	Hash        string       `json:"hash"`
	Orders      int          `json:"orders"`
	KnownOrders int          `json:"known_orders"`
	Bids        []book.Level `json:"bids"`
	Asks        []book.Level `json:"asks"`
}

func (e *Engine) State() State {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.book == nil {
		return State{Symbol: e.Symbol}
	}
	bids, asks := e.book.Depth(stateDepth)
	return State{
		Symbol:      e.Symbol,
		LastSeq:     e.book.LastSeq(),
		Hash:        e.book.HashHex(),
		Orders:      e.book.OrderCount(),
		KnownOrders: e.book.KnownOrderCount(),
		Bids:        bids,
		Asks:        asks,
	}
}

func (e *Engine) Run(ctx context.Context) error {
	if e.SnapshotEvery == 0 {
		e.SnapshotEvery = defaultSnapshotEvery
	}

	lock, err := db.TryLock(ctx, e.DB, "khan-engine:"+e.Symbol)
	if err != nil {
		return fmt.Errorf("движок для %s уже запущен или база недоступна: %w", e.Symbol, err)
	}
	defer lock.Release()

	if err := e.restore(ctx); err != nil {
		return err
	}
	if err := e.checkStreamCoversBook(ctx); err != nil {
		return err
	}

	e.Log.Info("движок запускается",
		"symbol", e.Symbol, "from_seq", e.lastSeq()+1, "hash", e.hashHex())

	consumer, err := e.createConsumer(ctx)
	if err != nil {
		return err
	}

	lastLockCheck := time.Now()
	for ctx.Err() == nil {
		if time.Since(lastLockCheck) >= lockCheckInterval {
			if err := lock.Check(ctx); err != nil {
				return err
			}
			lastLockCheck = time.Now()
		}

		batch, err := consumer.Fetch(fetchBatchSize, jetstream.FetchMaxWait(fetchWait))
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			e.Log.Warn("не удалось получить команды, пересоздаём consumer", "err", err)
			sleep(ctx, 200*time.Millisecond)
			if recreated, err := e.createConsumer(ctx); err == nil {
				consumer = recreated
			}
			continue
		}

		if err := e.processBatch(ctx, batch); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (e *Engine) processBatch(ctx context.Context, batch jetstream.MessageBatch) error {
	for message := range batch.Messages() {
		if err := e.handle(ctx, message); err != nil {
			return err
		}
	}
	if err := batch.Error(); err != nil && !errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
		e.Log.Debug("пачка завершилась с ошибкой", "err", err)
	}

	if err := e.flush(ctx); err != nil {
		return err
	}
	if e.commandsSinceSnapshot >= e.SnapshotEvery {
		return e.saveSnapshot(ctx)
	}
	return nil
}

func (e *Engine) createConsumer(ctx context.Context) (jetstream.Consumer, error) {
	consumer, err := e.JS.CreateConsumer(ctx, events.StreamOrders, jetstream.ConsumerConfig{
		FilterSubject:     events.OrdersSubject(e.Symbol),
		DeliverPolicy:     jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:       e.lastSeq() + 1,
		AckPolicy:         jetstream.AckNonePolicy,
		InactiveThreshold: 30 * time.Second,
		MemoryStorage:     true,
	})
	if err != nil {
		return nil, fmt.Errorf("не удалось создать consumer для ORDERS: %w", err)
	}
	return consumer, nil
}

func (e *Engine) restore(ctx context.Context) error {
	var state []byte
	var savedHash, version string
	err := e.DB.QueryRow(ctx, `
		SELECT state, hash, engine_version FROM book_snapshots
		WHERE symbol = $1
		ORDER BY last_seq DESC
		LIMIT 1`, e.Symbol).Scan(&state, &savedHash, &version)

	if errors.Is(err, pgx.ErrNoRows) {
		e.setBook(book.NewBook(e.Symbol))
		return nil
	}
	if err != nil {
		return fmt.Errorf("не удалось загрузить снапшот: %w", err)
	}

	if version != book.Version {
		e.Log.Warn("версия снапшота не совпадает с версией движка, проигрываем стрим с начала",
			"snapshot_version", version, "engine_version", book.Version)
		e.setBook(book.NewBook(e.Symbol))
		return nil
	}

	restored, err := book.Restore(state)
	if err != nil {
		return fmt.Errorf("не удалось восстановить стакан из снапшота: %w", err)
	}
	if restored.Symbol() != e.Symbol {
		return fmt.Errorf("%w: %s вместо %s", ErrSnapshotSymbol, restored.Symbol(), e.Symbol)
	}
	if restored.HashHex() != savedHash {
		return fmt.Errorf("%w: seq %d", ErrSnapshotHash, restored.LastSeq())
	}
	e.setBook(restored)
	return nil
}

func (e *Engine) setBook(restored *book.Book) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.book = restored
}

func (e *Engine) checkStreamCoversBook(ctx context.Context) error {
	stream, err := e.JS.Stream(ctx, events.StreamOrders)
	if err != nil {
		return fmt.Errorf("не удалось открыть стрим ORDERS: %w", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return fmt.Errorf("не удалось прочитать состояние стрима ORDERS: %w", err)
	}

	return checkCoverage(e.lastSeq(), info.State.FirstSeq, info.State.LastSeq)
}

func checkCoverage(bookSeq, streamFirstSeq, streamLastSeq uint64) error {
	if streamLastSeq < bookSeq {
		return fmt.Errorf("%w: в стриме последний seq %d, в снапшоте %d", ErrStreamBehind, streamLastSeq, bookSeq)
	}
	if streamFirstSeq > bookSeq+1 {
		return fmt.Errorf("%w: нужен seq %d, а в стриме первый %d", ErrReplayGap, bookSeq+1, streamFirstSeq)
	}
	return nil
}

func (e *Engine) handle(ctx context.Context, message jetstream.Msg) error {
	metadata, err := message.Metadata()
	if err != nil {
		return fmt.Errorf("не удалось прочитать метаданные сообщения: %w", err)
	}
	seq := metadata.Sequence.Stream
	if seq <= e.lastSeq() {
		return nil
	}

	var command events.Command
	if err := json.Unmarshal(message.Data(), &command); err != nil {
		e.Log.Error("команда не читается, отправляем в DLQ", "seq", seq, "err", err)
		e.sendToDLQ(ctx, message.Data(), "decode: "+err.Error())
		return e.skip(seq)
	}

	results, err := e.apply(seq, command)
	if err != nil {
		return err
	}
	e.commandsSinceSnapshot++

	for _, result := range results {
		if err := e.publish(result); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) skip(seq uint64) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.book.Skip(seq); err != nil {
		return err
	}
	e.commandsSinceSnapshot++
	return nil
}

func (e *Engine) apply(seq uint64, command events.Command) ([]events.Exec, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	switch command.Type {
	case events.CmdNew:
		return e.applyNewOrder(seq, command)
	case events.CmdCancel:
		return e.applyCancel(seq, command)
	default:
		e.Log.Error("неизвестный тип команды, пропускаем", "seq", seq, "type", command.Type)
		return nil, e.book.Skip(seq)
	}
}

func (e *Engine) applyNewOrder(seq uint64, command events.Command) ([]events.Exec, error) {
	side, _ := book.ParseSide(command.Side)
	order := &book.Order{ID: command.OrderID, Seq: seq, Side: side, Price: command.Price, Qty: command.Qty}

	trades, err := e.book.Add(order)
	if errors.Is(err, book.ErrSeqNotIncreased) {
		return nil, err
	}
	if errors.Is(err, book.ErrDuplicateID) {
		e.Log.Warn("повторная команда new для известной заявки, пропускаем", "seq", seq, "order_id", command.OrderID)
		return nil, nil
	}
	if err != nil {
		rejected := events.Exec{
			Type:    events.ExecRejected,
			ID:      events.RejectedID(e.Symbol, seq),
			Symbol:  e.Symbol,
			Seq:     seq,
			OrderID: command.OrderID,
			Reason:  err.Error(),
		}
		return []events.Exec{rejected}, nil
	}

	results := make([]events.Exec, 0, len(trades)+1)
	results = append(results, events.Exec{
		Type:    events.ExecAccepted,
		ID:      events.AcceptedID(e.Symbol, seq),
		Symbol:  e.Symbol,
		Seq:     seq,
		OrderID: command.OrderID,
	})
	for _, trade := range trades {
		results = append(results, events.Exec{
			Type:         events.ExecTrade,
			ID:           trade.ID,
			Symbol:       e.Symbol,
			Seq:          trade.Seq,
			Price:        trade.Price,
			Qty:          trade.Qty,
			MakerOrderID: trade.MakerID,
			TakerOrderID: trade.TakerID,
			TakerSide:    trade.TakerSide.String(),
		})
	}
	return results, nil
}

func (e *Engine) applyCancel(seq uint64, command events.Command) ([]events.Exec, error) {
	remaining, err := e.book.Cancel(seq, command.OrderID)
	if errors.Is(err, book.ErrOrderNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	cancelled := events.Exec{
		Type:      events.ExecCancelled,
		ID:        events.CancelledID(e.Symbol, seq),
		Symbol:    e.Symbol,
		Seq:       seq,
		OrderID:   command.OrderID,
		Remaining: remaining,
	}
	return []events.Exec{cancelled}, nil
}

func (e *Engine) publish(result events.Exec) error {
	data, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("не удалось сериализовать событие %s: %w", result.ID, err)
	}
	future, err := e.JS.PublishAsync(events.TradesSubject(e.Symbol), data, jetstream.WithMsgID(result.ID))
	if err != nil {
		return fmt.Errorf("не удалось опубликовать событие %s: %w", result.ID, err)
	}
	e.pending = append(e.pending, future)
	return nil
}

func (e *Engine) flush(ctx context.Context) error {
	timeout := time.NewTimer(publishAckTimeout)
	defer timeout.Stop()

	for _, future := range e.pending {
		select {
		case <-future.Ok():
		case err := <-future.Err():
			return fmt.Errorf("%w: %w", ErrPublishNotConfirmed, err)
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout.C:
			return fmt.Errorf("%w: истекло время ожидания", ErrPublishNotConfirmed)
		}
	}
	e.pending = e.pending[:0]
	return nil
}

func (e *Engine) saveSnapshot(ctx context.Context) error {
	e.mu.RLock()
	data, err := e.book.Snapshot()
	seq := e.book.LastSeq()
	hash := e.book.HashHex()
	e.mu.RUnlock()
	if err != nil {
		return fmt.Errorf("не удалось сериализовать стакан: %w", err)
	}

	_, err = e.DB.Exec(ctx, `
		INSERT INTO book_snapshots (symbol, last_seq, state, hash, engine_version)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT DO NOTHING`, e.Symbol, seq, data, hash, book.Version)
	if err != nil {
		return fmt.Errorf("не удалось сохранить снапшот: %w", err)
	}

	_, err = e.DB.Exec(ctx, `
		DELETE FROM book_snapshots
		WHERE symbol = $1 AND last_seq < (
			SELECT min(last_seq) FROM (
				SELECT last_seq FROM book_snapshots
				WHERE symbol = $1
				ORDER BY last_seq DESC
				LIMIT $2
			) AS newest
		)`, e.Symbol, snapshotsToKeep)
	if err != nil {
		e.Log.Warn("не удалось удалить старые снапшоты", "err", err)
	}

	e.commandsSinceSnapshot = 0
	e.Log.Info("снапшот сохранён", "seq", seq, "hash", hash[:16], "bytes", len(data))
	return nil
}

func (e *Engine) sendToDLQ(ctx context.Context, data []byte, reason string) {
	dlqContext, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	subject := "dlq.engine." + e.Symbol
	if err := streams.PublishDLQ(dlqContext, e.JS, subject, data, reason); err != nil {
		e.Log.Error("не удалось записать команду в DLQ", "err", err)
	}
}

func (e *Engine) lastSeq() uint64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.book.LastSeq()
}

func (e *Engine) hashHex() string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.book.HashHex()
}

func sleep(ctx context.Context, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
