package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/HowkaCoder/khan/internal/db"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func doRequest(t *testing.T, handler http.Handler, method, path, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

func TestRequestValidationWithoutDatabase(t *testing.T) {
	handler := New(nil, []string{"ACME-USD"}, quietLogger()).Routes()
	key := map[string]string{"Idempotency-Key": "k1"}
	validOrder := `{"account_id":1,"symbol":"ACME-USD","side":"buy","price":"101.00","qty":3}`

	cases := []struct {
		name    string
		method  string
		path    string
		body    string
		headers map[string]string
	}{
		{"missing idempotency key", "POST", "/v1/orders", validOrder, nil},
		{"too long idempotency key", "POST", "/v1/orders", validOrder, map[string]string{"Idempotency-Key": strings.Repeat("k", 129)}},
		{"broken json", "POST", "/v1/orders", `{"account_id":`, key},
		{"unknown field", "POST", "/v1/orders", `{"account_id":1,"symbol":"ACME-USD","side":"buy","price":"1","qty":1,"extra":1}`, key},
		{"two objects", "POST", "/v1/orders", validOrder + validOrder, key},
		{"price as number", "POST", "/v1/orders", `{"account_id":1,"symbol":"ACME-USD","side":"buy","price":101,"qty":1}`, key},
		{"zero account", "POST", "/v1/orders", `{"account_id":0,"symbol":"ACME-USD","side":"buy","price":"1","qty":1}`, key},
		{"unknown symbol", "POST", "/v1/orders", `{"account_id":1,"symbol":"XYZ-USD","side":"buy","price":"1","qty":1}`, key},
		{"bad side", "POST", "/v1/orders", `{"account_id":1,"symbol":"ACME-USD","side":"hold","price":"1","qty":1}`, key},
		{"zero price", "POST", "/v1/orders", `{"account_id":1,"symbol":"ACME-USD","side":"buy","price":"0.00","qty":1}`, key},
		{"three decimals", "POST", "/v1/orders", `{"account_id":1,"symbol":"ACME-USD","side":"buy","price":"1.001","qty":1}`, key},
		{"zero qty", "POST", "/v1/orders", `{"account_id":1,"symbol":"ACME-USD","side":"buy","price":"1","qty":0}`, key},
		{"overflow notional", "POST", "/v1/orders", `{"account_id":1,"symbol":"ACME-USD","side":"buy","price":"90000000000000000.00","qty":2}`, key},
		{"huge body", "POST", "/v1/orders", `{"symbol":"` + strings.Repeat("A", maxBodyBytes) + `"}`, key},
		{"deposit unknown asset", "POST", "/v1/deposits", `{"account_id":1,"asset":"EUR","amount":5}`, nil},
		{"deposit system account", "POST", "/v1/deposits", `{"account_id":0,"asset":"USD","amount":5}`, nil},
		{"deposit negative", "POST", "/v1/deposits", `{"account_id":1,"asset":"USD","amount":-5}`, nil},
		{"balances bad id", "GET", "/v1/accounts/abc/balances", "", nil},
		{"balances zero id", "GET", "/v1/accounts/0/balances", "", nil},
		{"active orders negative id", "GET", "/v1/accounts/-3/orders", "", nil},
		{"cancel without account", "DELETE", "/v1/orders/ord_1", "", nil},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			response := doRequest(t, handler, testCase.method, testCase.path, testCase.body, testCase.headers)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status %d, body %s", response.Code, response.Body.String())
			}
			var body apiError
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil || body.Error == "" {
				t.Fatalf("error body %q", response.Body.String())
			}
			if response.Header().Get(requestIDHeader) == "" {
				t.Fatal("request id header is missing")
			}
		})
	}
}

func TestDepositsCanBeDisabled(t *testing.T) {
	server := New(nil, []string{"ACME-USD"}, quietLogger())
	server.DepositsEnabled = false
	response := doRequest(t, server.Routes(), "POST", "/v1/deposits", `{"account_id":1,"asset":"USD","amount":5}`, nil)
	if response.Code != http.StatusNotFound {
		t.Fatalf("status %d", response.Code)
	}
}

func testPool(t *testing.T) *pgxpool.Pool {
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
	t.Cleanup(pool.Close)
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	return pool
}

func decodeBody[T any](t *testing.T, response *httptest.ResponseRecorder) T {
	t.Helper()
	var value T
	if err := json.Unmarshal(response.Body.Bytes(), &value); err != nil {
		t.Fatalf("decode %q: %v", response.Body.String(), err)
	}
	return value
}

func TestOrderFlowWithDatabase(t *testing.T) {
	pool := testPool(t)
	handler := New(pool, []string{"ACME-USD"}, quietLogger()).Routes()
	account := time.Now().UnixNano()%1e9 + 50_000
	other := account + 1
	key := fmt.Sprintf("key-%d", account)

	deposit := fmt.Sprintf(`{"account_id":%d,"asset":"USD","amount":30300}`, account)
	if response := doRequest(t, handler, "POST", "/v1/deposits", deposit, nil); response.Code != http.StatusCreated {
		t.Fatalf("deposit status %d: %s", response.Code, response.Body.String())
	}

	order := fmt.Sprintf(`{"account_id":%d,"symbol":"ACME-USD","side":"buy","price":"101.00","qty":3}`, account)
	first := doRequest(t, handler, "POST", "/v1/orders", order, map[string]string{"Idempotency-Key": key})
	if first.Code != http.StatusAccepted {
		t.Fatalf("first order status %d: %s", first.Code, first.Body.String())
	}
	created := decodeBody[orderResponse](t, first)

	repeated := doRequest(t, handler, "POST", "/v1/orders", order, map[string]string{"Idempotency-Key": key})
	if repeated.Code != http.StatusOK || decodeBody[orderResponse](t, repeated).ID != created.ID {
		t.Fatalf("repeat status %d: %s", repeated.Code, repeated.Body.String())
	}

	changed := strings.Replace(order, `"qty":3`, `"qty":2`, 1)
	if response := doRequest(t, handler, "POST", "/v1/orders", changed, map[string]string{"Idempotency-Key": key}); response.Code != http.StatusConflict {
		t.Fatalf("reused key with other body: status %d", response.Code)
	}

	if response := doRequest(t, handler, "POST", "/v1/orders", order, map[string]string{"Idempotency-Key": key + "-2"}); response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("second reserve without money: status %d", response.Code)
	}

	balances := decodeBody[[]balanceResponse](t, doRequest(t, handler, "GET", fmt.Sprintf("/v1/accounts/%d/balances", account), "", nil))
	if len(balances) != 1 || balances[0].Available != 0 || balances[0].Held != 30300 {
		t.Fatalf("balances %+v", balances)
	}

	active := decodeBody[[]orderResponse](t, doRequest(t, handler, "GET", fmt.Sprintf("/v1/accounts/%d/orders", account), "", nil))
	if len(active) != 1 || active[0].ID != created.ID || active[0].Status != "pending" || active[0].Price != "101.00" {
		t.Fatalf("active orders %+v", active)
	}

	if response := doRequest(t, handler, "DELETE", fmt.Sprintf("/v1/orders/%s?account_id=%d", created.ID, other), "", nil); response.Code != http.StatusNotFound {
		t.Fatalf("cancel by other account: status %d", response.Code)
	}
	for i := 0; i < 2; i++ {
		response := doRequest(t, handler, "DELETE", fmt.Sprintf("/v1/orders/%s?account_id=%d", created.ID, account), "", nil)
		if response.Code != http.StatusAccepted {
			t.Fatalf("cancel attempt %d: status %d", i, response.Code)
		}
	}
	var cancelCommands int
	err := pool.QueryRow(context.Background(), `SELECT count(*) FROM outbox WHERE msg_id = $1`, "cxl-"+created.ID).Scan(&cancelCommands)
	if err != nil || cancelCommands != 1 {
		t.Fatalf("cancel commands %d, err %v", cancelCommands, err)
	}

	if response := doRequest(t, handler, "GET", "/v1/orders/ord_missing", "", nil); response.Code != http.StatusNotFound {
		t.Fatalf("missing order status %d", response.Code)
	}
}

func TestDepositOverflowIsClientError(t *testing.T) {
	pool := testPool(t)
	handler := New(pool, []string{"ACME-USD"}, quietLogger()).Routes()
	account := time.Now().UnixNano()%1e9 + 70_000
	body := fmt.Sprintf(`{"account_id":%d,"asset":"ACME","amount":9223372036854775807}`, account)

	if response := doRequest(t, handler, "POST", "/v1/deposits", body, nil); response.Code != http.StatusCreated {
		t.Fatalf("first deposit status %d", response.Code)
	}
	response := doRequest(t, handler, "POST", "/v1/deposits", body, nil)
	if response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("overflow deposit status %d: %s", response.Code, response.Body.String())
	}
}
