package api

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/HowkaCoder/khan/internal/events"
	"github.com/HowkaCoder/khan/internal/money"
)

const (
	maxBodyBytes          = 64 << 10
	maxIdempotencyKeyLen  = 128
	activeOrdersLimit     = 100
	pgNumericOutOfRange   = "22003"
	requestIDHeader       = "X-Request-ID"
	maxIncomingRequestLen = 64
)

var (
	errInsufficientFunds = errors.New("недостаточно средств")
	errKeyReused         = errors.New("ключ идемпотентности уже использован с другими параметрами")
)

type Server struct {
	DepositsEnabled bool

	db      *pgxpool.Pool
	symbols map[string]bool
	assets  map[string]bool
	log     *slog.Logger
}

func New(db *pgxpool.Pool, symbols []string, log *slog.Logger) *Server {
	server := &Server{
		DepositsEnabled: true,
		db:              db,
		symbols:         map[string]bool{},
		assets:          map[string]bool{},
		log:             log,
	}
	for _, symbol := range symbols {
		base, quote, err := money.SplitSymbol(symbol)
		if err != nil {
			log.Warn("торговая пара пропущена", "symbol", symbol, "err", err)
			continue
		}
		server.symbols[symbol] = true
		server.assets[base] = true
		server.assets[quote] = true
	}
	return server
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("GET /readyz", s.ready)
	mux.HandleFunc("POST /v1/deposits", s.deposit)
	mux.HandleFunc("POST /v1/orders", s.placeOrder)
	mux.HandleFunc("GET /v1/orders/{id}", s.getOrder)
	mux.HandleFunc("DELETE /v1/orders/{id}", s.cancelOrder)
	mux.HandleFunc("GET /v1/accounts/{id}/balances", s.balances)
	mux.HandleFunc("GET /v1/accounts/{id}/orders", s.activeOrders)
	return s.withRequestLog(mux)
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (recorder *statusRecorder) WriteHeader(status int) {
	recorder.status = status
	recorder.ResponseWriter.WriteHeader(status)
}

func (s *Server) withRequestLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := r.Header.Get(requestIDHeader)
		if requestID == "" || len(requestID) > maxIncomingRequestLen {
			requestID = randomHex(8)
		}
		w.Header().Set(requestIDHeader, requestID)

		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		started := time.Now()
		next.ServeHTTP(recorder, r)

		s.log.Info("http запрос",
			"request_id", requestID,
			"method", r.Method,
			"path", r.URL.Path,
			"status", recorder.status,
			"duration_ms", time.Since(started).Milliseconds())
	})
}

type apiError struct {
	Error string `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func fail(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, apiError{Error: message})
}

func (s *Server) internalError(w http.ResponseWriter, r *http.Request, err error) {
	s.log.Error("внутренняя ошибка", "request_id", w.Header().Get(requestIDHeader), "path", r.URL.Path, "err", err)
	fail(w, http.StatusInternalServerError, "внутренняя ошибка сервера")
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("после JSON-объекта есть лишние данные")
	}
	return nil
}

func randomHex(byteCount int) string {
	buffer := make([]byte, byteCount)
	_, _ = rand.Read(buffer)
	return hex.EncodeToString(buffer)
}

func newOrderID() string {
	return "ord_" + randomHex(12)
}

func parsePositiveID(text string) (int64, bool) {
	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil || value <= 0 {
		return 0, false
	}
	return value, true
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.db.Ping(ctx); err != nil {
		s.log.Warn("база недоступна для readiness", "err", err)
		fail(w, http.StatusServiceUnavailable, "база данных недоступна")
		return
	}
	w.WriteHeader(http.StatusOK)
}

type depositRequest struct {
	AccountID int64  `json:"account_id"`
	Asset     string `json:"asset"`
	Amount    int64  `json:"amount"`
}

func (s *Server) deposit(w http.ResponseWriter, r *http.Request) {
	if !s.DepositsEnabled {
		fail(w, http.StatusNotFound, "тестовые пополнения выключены")
		return
	}

	var request depositRequest
	if err := decodeJSON(w, r, &request); err != nil {
		fail(w, http.StatusBadRequest, "некорректный JSON: ожидаются поля account_id, asset, amount")
		return
	}
	if request.AccountID <= 0 {
		fail(w, http.StatusBadRequest, "account_id должен быть больше нуля")
		return
	}
	if !s.assets[request.Asset] {
		fail(w, http.StatusBadRequest, "неизвестный актив")
		return
	}
	if request.Amount <= 0 {
		fail(w, http.StatusBadRequest, "amount должен быть больше нуля (в минимальных единицах)")
		return
	}

	ref := "dep_" + randomHex(12)
	err := pgx.BeginFunc(r.Context(), s.db, func(tx pgx.Tx) error {
		return recordDeposit(r.Context(), tx, ref, request)
	})
	if isNumericOverflow(err) {
		fail(w, http.StatusUnprocessableEntity, "баланс после пополнения стал бы слишком большим")
		return
	}
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"ref": ref})
}

func recordDeposit(ctx context.Context, tx pgx.Tx, ref string, request depositRequest) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO ledger_entries (ref, account_id, asset, amount)
		VALUES ($1, 0, $2, -$3::bigint), ($1, $4, $2, $3::bigint)`,
		ref, request.Asset, request.Amount, request.AccountID)
	if err != nil {
		return fmt.Errorf("не удалось записать проводки пополнения: %w", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO balances (account_id, asset, available) VALUES ($1, $2, $3)
		ON CONFLICT (account_id, asset)
		DO UPDATE SET available = balances.available + EXCLUDED.available`,
		request.AccountID, request.Asset, request.Amount)
	if err != nil {
		return fmt.Errorf("не удалось увеличить баланс: %w", err)
	}
	return nil
}

func isNumericOverflow(err error) bool {
	var pgError *pgconn.PgError
	return errors.As(err, &pgError) && pgError.Code == pgNumericOutOfRange
}

type orderRequest struct {
	AccountID int64  `json:"account_id"`
	Symbol    string `json:"symbol"`
	Side      string `json:"side"`
	Price     string `json:"price"`
	Qty       int64  `json:"qty"`
}

type validOrder struct {
	accountID   int64
	symbol      string
	side        string
	price       int64
	qty         int64
	holdAsset   string
	holdAmount  int64
	requestHash string
}

type orderResponse struct {
	ID        string `json:"id"`
	AccountID int64  `json:"account_id"`
	Symbol    string `json:"symbol"`
	Side      string `json:"side"`
	Price     string `json:"price"`
	Qty       int64  `json:"qty"`
	Filled    int64  `json:"filled"`
	Cancelled int64  `json:"cancelled"`
	Status    string `json:"status"`
}

func (s *Server) validateOrder(request orderRequest) (validOrder, string) {
	if request.AccountID <= 0 {
		return validOrder{}, "account_id должен быть больше нуля"
	}
	if !s.symbols[request.Symbol] {
		return validOrder{}, "неизвестная торговая пара"
	}
	if request.Side != "buy" && request.Side != "sell" {
		return validOrder{}, `side должен быть "buy" или "sell"`
	}
	price, err := money.Parse(request.Price, money.PriceScale)
	if err != nil || price <= 0 {
		return validOrder{}, "price должен быть положительным десятичным числом не более чем с 2 знаками после точки"
	}
	if request.Qty <= 0 {
		return validOrder{}, "qty должен быть положительным целым числом"
	}

	base, quote, _ := money.SplitSymbol(request.Symbol)
	order := validOrder{
		accountID:  request.AccountID,
		symbol:     request.Symbol,
		side:       request.Side,
		price:      price,
		qty:        request.Qty,
		holdAsset:  base,
		holdAmount: request.Qty,
	}
	if request.Side == "buy" {
		order.holdAsset = quote
		order.holdAmount, err = money.Mul(price, request.Qty)
		if err != nil {
			return validOrder{}, "price × qty слишком велико"
		}
	}

	fingerprint := fmt.Sprintf("%d|%s|%s|%d|%d", order.accountID, order.symbol, order.side, order.price, order.qty)
	hash := sha256.Sum256([]byte(fingerprint))
	order.requestHash = hex.EncodeToString(hash[:])
	return order, ""
}

func (s *Server) placeOrder(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" || len(key) > maxIdempotencyKeyLen {
		fail(w, http.StatusBadRequest, "нужен заголовок Idempotency-Key длиной до 128 символов")
		return
	}

	var request orderRequest
	if err := decodeJSON(w, r, &request); err != nil {
		fail(w, http.StatusBadRequest, "некорректный JSON заявки")
		return
	}
	order, problem := s.validateOrder(request)
	if problem != "" {
		fail(w, http.StatusBadRequest, problem)
		return
	}

	orderID := newOrderID()
	var existingOrderID string
	err := pgx.BeginFunc(r.Context(), s.db, func(tx pgx.Tx) error {
		var err error
		existingOrderID, err = saveOrder(r.Context(), tx, key, orderID, order)
		return err
	})

	switch {
	case errors.Is(err, errInsufficientFunds):
		message := fmt.Sprintf("недостаточно средств: нужно %d %s (в минимальных единицах)", order.holdAmount, order.holdAsset)
		fail(w, http.StatusUnprocessableEntity, message)
		return
	case errors.Is(err, errKeyReused):
		fail(w, http.StatusConflict, errKeyReused.Error())
		return
	case isNumericOverflow(err):
		fail(w, http.StatusUnprocessableEntity, "сумма резерва слишком велика")
		return
	case err != nil:
		s.internalError(w, r, err)
		return
	}

	if existingOrderID != "" {
		existing, err := loadOrder(r.Context(), s.db, existingOrderID)
		if err != nil {
			s.internalError(w, r, err)
			return
		}
		writeJSON(w, http.StatusOK, existing)
		return
	}

	writeJSON(w, http.StatusAccepted, orderResponse{
		ID:        orderID,
		AccountID: order.accountID,
		Symbol:    order.symbol,
		Side:      order.side,
		Price:     money.Format(order.price, money.PriceScale),
		Qty:       order.qty,
		Status:    "pending",
	})
}

func saveOrder(ctx context.Context, tx pgx.Tx, key, orderID string, order validOrder) (string, error) {
	tag, err := tx.Exec(ctx, `
		INSERT INTO idempotency_keys (account_id, key, order_id, request_hash)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT DO NOTHING`, order.accountID, key, orderID, order.requestHash)
	if err != nil {
		return "", fmt.Errorf("не удалось сохранить ключ идемпотентности: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return findPreviousOrder(ctx, tx, key, order)
	}

	tag, err = tx.Exec(ctx, `
		UPDATE balances
		SET available = available - $3, held = held + $3
		WHERE account_id = $1 AND asset = $2 AND available >= $3`,
		order.accountID, order.holdAsset, order.holdAmount)
	if err != nil {
		return "", fmt.Errorf("не удалось зарезервировать средства: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return "", errInsufficientFunds
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO orders (id, account_id, symbol, side, price, qty)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		orderID, order.accountID, order.symbol, order.side, order.price, order.qty)
	if err != nil {
		return "", fmt.Errorf("не удалось сохранить заявку: %w", err)
	}

	payload, err := json.Marshal(events.Command{
		Type:    events.CmdNew,
		OrderID: orderID,
		Side:    order.side,
		Price:   order.price,
		Qty:     order.qty,
	})
	if err != nil {
		return "", err
	}
	_, err = tx.Exec(ctx, `INSERT INTO outbox (subject, msg_id, payload) VALUES ($1, $2, $3)`,
		events.OrdersSubject(order.symbol), events.NewOrderMsgID(orderID), payload)
	if err != nil {
		return "", fmt.Errorf("не удалось записать команду в outbox: %w", err)
	}
	return "", nil
}

func findPreviousOrder(ctx context.Context, tx pgx.Tx, key string, order validOrder) (string, error) {
	var previousOrderID, previousHash string
	err := tx.QueryRow(ctx, `
		SELECT order_id, request_hash FROM idempotency_keys
		WHERE account_id = $1 AND key = $2`, order.accountID, key).Scan(&previousOrderID, &previousHash)
	if err != nil {
		return "", fmt.Errorf("не удалось прочитать ключ идемпотентности: %w", err)
	}
	if previousHash != "" && previousHash != order.requestHash {
		return "", errKeyReused
	}
	return previousOrderID, nil
}

const orderColumns = `id, account_id, symbol, side, price, qty, filled, cancelled_qty, status`

func scanOrder(row pgx.Row) (orderResponse, error) {
	var order orderResponse
	var price int64
	err := row.Scan(&order.ID, &order.AccountID, &order.Symbol, &order.Side, &price,
		&order.Qty, &order.Filled, &order.Cancelled, &order.Status)
	if err != nil {
		return orderResponse{}, err
	}
	order.Price = money.Format(price, money.PriceScale)
	return order, nil
}

func loadOrder(ctx context.Context, pool *pgxpool.Pool, orderID string) (orderResponse, error) {
	row := pool.QueryRow(ctx, `SELECT `+orderColumns+` FROM orders WHERE id = $1`, orderID)
	return scanOrder(row)
}

func (s *Server) getOrder(w http.ResponseWriter, r *http.Request) {
	order, err := loadOrder(r.Context(), s.db, r.PathValue("id"))
	if errors.Is(err, pgx.ErrNoRows) {
		fail(w, http.StatusNotFound, "заявка не найдена")
		return
	}
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, order)
}

type balanceResponse struct {
	Asset     string `json:"asset"`
	Available int64  `json:"available"`
	Held      int64  `json:"held"`
}

func (s *Server) balances(w http.ResponseWriter, r *http.Request) {
	accountID, ok := parsePositiveID(r.PathValue("id"))
	if !ok {
		fail(w, http.StatusBadRequest, "номер счёта должен быть положительным целым числом")
		return
	}
	rows, err := s.db.Query(r.Context(), `
		SELECT asset, available, held FROM balances
		WHERE account_id = $1
		ORDER BY asset`, accountID)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	result, err := pgx.CollectRows(rows, pgx.RowToStructByPos[balanceResponse])
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) activeOrders(w http.ResponseWriter, r *http.Request) {
	accountID, ok := parsePositiveID(r.PathValue("id"))
	if !ok {
		fail(w, http.StatusBadRequest, "номер счёта должен быть положительным целым числом")
		return
	}
	rows, err := s.db.Query(r.Context(), `
		SELECT `+orderColumns+` FROM orders
		WHERE account_id = $1 AND status IN ('pending', 'open', 'partially_filled')
		ORDER BY created_at DESC
		LIMIT $2`, accountID, activeOrdersLimit)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	defer rows.Close()

	result := make([]orderResponse, 0)
	for rows.Next() {
		order, err := scanOrder(rows)
		if err != nil {
			s.internalError(w, r, err)
			return
		}
		result = append(result, order)
	}
	if err := rows.Err(); err != nil {
		s.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func isActiveStatus(status string) bool {
	return status == "pending" || status == "open" || status == "partially_filled"
}

func (s *Server) cancelOrder(w http.ResponseWriter, r *http.Request) {
	orderID := r.PathValue("id")
	accountID, ok := parsePositiveID(r.URL.Query().Get("account_id"))
	if !ok {
		fail(w, http.StatusBadRequest, "нужен параметр account_id — положительное целое число")
		return
	}

	var status string
	var found bool
	err := pgx.BeginFunc(r.Context(), s.db, func(tx pgx.Tx) error {
		var err error
		status, found, err = requestCancel(r.Context(), tx, orderID, accountID)
		return err
	})

	switch {
	case err != nil:
		s.internalError(w, r, err)
	case !found:
		fail(w, http.StatusNotFound, "заявка не найдена")
	case !isActiveStatus(status):
		fail(w, http.StatusConflict, "заявка уже в статусе "+status)
	default:
		writeJSON(w, http.StatusAccepted, map[string]string{"id": orderID, "status": "cancel_requested"})
	}
}

func requestCancel(ctx context.Context, tx pgx.Tx, orderID string, accountID int64) (string, bool, error) {
	var owner int64
	var symbol, status string
	err := tx.QueryRow(ctx, `
		SELECT account_id, symbol, status FROM orders
		WHERE id = $1
		FOR UPDATE`, orderID).Scan(&owner, &symbol, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("не удалось прочитать заявку: %w", err)
	}
	if owner != accountID {
		return "", false, nil
	}
	if !isActiveStatus(status) {
		return status, true, nil
	}

	payload, err := json.Marshal(events.Command{Type: events.CmdCancel, OrderID: orderID})
	if err != nil {
		return "", false, err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox (subject, msg_id, payload) VALUES ($1, $2, $3)
		ON CONFLICT (msg_id) DO NOTHING`,
		events.OrdersSubject(symbol), events.CancelOrderMsgID(orderID), payload)
	if err != nil {
		return "", false, fmt.Errorf("не удалось записать команду отмены в outbox: %w", err)
	}
	return status, true, nil
}
