package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type options struct {
	apiURL        string
	symbol        string
	accounts      int
	orders        int
	clients       int
	seed          int64
	cancelPercent int
}

type placedOrder struct {
	id      string
	account int
}

type generator struct {
	options options
	client  *http.Client
	runID   int64

	mu     sync.Mutex
	placed []placedOrder

	nextIndex atomic.Int64
	failed    atomic.Int64
	cancels   atomic.Int64
	latencies []time.Duration
}

func main() {
	var opts options
	flag.StringVar(&opts.apiURL, "api", "http://localhost:8080", "адрес API")
	flag.StringVar(&opts.symbol, "symbol", "ACME-USD", "торговая пара")
	flag.IntVar(&opts.accounts, "accounts", 20, "количество счетов")
	flag.IntVar(&opts.orders, "orders", 2000, "сколько заявок отправить")
	flag.IntVar(&opts.clients, "c", 8, "сколько клиентов работают одновременно")
	flag.Int64Var(&opts.seed, "seed", 1, "seed генератора случайных чисел")
	flag.IntVar(&opts.cancelPercent, "cancel", 15, "процент запросов, которые отменяют раньше созданную заявку")
	flag.Parse()

	if opts.orders <= 0 || opts.clients <= 0 || opts.accounts <= 0 {
		fmt.Fprintln(os.Stderr, "orders, c и accounts должны быть больше нуля")
		os.Exit(2)
	}

	g := &generator{
		options:   opts,
		client:    &http.Client{Timeout: 10 * time.Second},
		runID:     time.Now().UnixNano(),
		latencies: make([]time.Duration, opts.orders),
	}
	if err := g.fundAccounts(); err != nil {
		fmt.Fprintln(os.Stderr, "пополнение не удалось:", err)
		os.Exit(1)
	}

	started := time.Now()
	var wg sync.WaitGroup
	for worker := 0; worker < opts.clients; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			g.runClient(rand.New(rand.NewSource(opts.seed + int64(worker))))
		}(worker)
	}
	wg.Wait()
	g.printReport(time.Since(started))
}

func (g *generator) fundAccounts() error {
	for account := 1; account <= g.options.accounts; account++ {
		for asset, amount := range map[string]int64{"USD": 100_000_000, "ACME": 1_000_000} {
			body := map[string]any{"account_id": account, "asset": asset, "amount": amount}
			status, err := g.post("/v1/deposits", body, "")
			if err != nil {
				return err
			}
			if status != http.StatusCreated {
				return fmt.Errorf("ответ %d для счёта %d", status, account)
			}
		}
	}
	return nil
}

func (g *generator) runClient(random *rand.Rand) {
	for {
		index := int(g.nextIndex.Add(1)) - 1
		if index >= g.options.orders {
			return
		}
		if random.Intn(100) < g.options.cancelPercent {
			g.cancelRandomOrder(random)
		}
		g.placeRandomOrder(random, index)
	}
}

func (g *generator) cancelRandomOrder(random *rand.Rand) {
	g.mu.Lock()
	if len(g.placed) == 0 {
		g.mu.Unlock()
		return
	}
	target := g.placed[random.Intn(len(g.placed))]
	g.mu.Unlock()

	url := fmt.Sprintf("%s/v1/orders/%s?account_id=%d", g.options.apiURL, target.id, target.account)
	request, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return
	}
	response, err := g.client.Do(request)
	if err != nil {
		return
	}
	response.Body.Close()
	g.cancels.Add(1)
}

func (g *generator) placeRandomOrder(random *rand.Rand, index int) {
	side := "buy"
	if random.Intn(2) == 0 {
		side = "sell"
	}
	priceCents := 9900 + random.Intn(21)*10
	account := 1 + random.Intn(g.options.accounts)
	body := map[string]any{
		"account_id": account,
		"symbol":     g.options.symbol,
		"side":       side,
		"price":      fmt.Sprintf("%d.%02d", priceCents/100, priceCents%100),
		"qty":        1 + random.Intn(10),
	}

	started := time.Now()
	orderID, status, err := g.placeOrder(body, fmt.Sprintf("lg-%d-%d", g.runID, index))
	g.latencies[index] = time.Since(started)
	if err != nil || status != http.StatusAccepted {
		g.failed.Add(1)
		return
	}

	g.mu.Lock()
	g.placed = append(g.placed, placedOrder{id: orderID, account: account})
	g.mu.Unlock()
}

func (g *generator) post(path string, body any, idempotencyKey string) (int, error) {
	response, err := g.send(path, body, idempotencyKey)
	if err != nil {
		return 0, err
	}
	response.Body.Close()
	return response.StatusCode, nil
}

func (g *generator) placeOrder(body any, idempotencyKey string) (string, int, error) {
	response, err := g.send("/v1/orders", body, idempotencyKey)
	if err != nil {
		return "", 0, err
	}
	defer response.Body.Close()

	var result struct {
		ID string `json:"id"`
	}
	_ = json.NewDecoder(response.Body).Decode(&result)
	return result.ID, response.StatusCode, nil
}

func (g *generator) send(path string, body any, idempotencyKey string) (*http.Response, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequest(http.MethodPost, g.options.apiURL+path, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	if idempotencyKey != "" {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	return g.client.Do(request)
}

func (g *generator) printReport(elapsed time.Duration) {
	latencies := g.latencies
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	percentile := func(p float64) time.Duration {
		return latencies[int(float64(len(latencies)-1)*p)].Round(time.Microsecond)
	}

	fmt.Printf("заявок: %d за %s (%.0f запросов/с), неуспешных: %d, запросов отмены: %d\n",
		g.options.orders, elapsed.Round(time.Millisecond), float64(g.options.orders)/elapsed.Seconds(),
		g.failed.Load(), g.cancels.Load())
	fmt.Printf("задержка POST /v1/orders на этой машине: p50 %s, p99 %s, max %s\n",
		percentile(0.50), percentile(0.99), latencies[len(latencies)-1].Round(time.Microsecond))
}
