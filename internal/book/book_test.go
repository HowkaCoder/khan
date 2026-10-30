package book

import (
	"encoding/json"
	"errors"
	"testing"
)

const sym = "ACME-USD"

func mustAdd(t *testing.T, b *Book, o *Order) []Trade {
	t.Helper()
	trades, err := b.Add(o)
	if err != nil {
		t.Fatalf("Add(%s): %v", o.ID, err)
	}
	checkInvariants(t, b)
	return trades
}

func checkInvariants(t testing.TB, b *Book) {
	t.Helper()
	bid, okB := b.BestBid()
	ask, okA := b.BestAsk()
	if okB && okA && bid >= ask {
		t.Fatalf("crossed book: bid %d >= ask %d", bid, ask)
	}
	count := 0
	for i, side := range [][]*level{b.bids, b.asks} {
		for j, l := range side {
			if len(l.orders) == 0 {
				t.Fatalf("empty level %d left in book", l.price)
			}
			if j > 0 {
				prev := side[j-1].price
				if (i == 0 && prev <= l.price) || (i == 1 && prev >= l.price) {
					t.Fatalf("levels out of order: %d then %d", prev, l.price)
				}
			}
			for k, o := range l.orders {
				count++
				if o.Remaining <= 0 || o.Remaining > o.Qty || o.Price != l.price {
					t.Fatalf("bad resting order %+v", *o)
				}
				if k > 0 && l.orders[k-1].Seq >= o.Seq {
					t.Fatalf("FIFO broken on level %d", l.price)
				}
				if b.resting[o.ID] != o {
					t.Fatalf("index out of sync for %s", o.ID)
				}
			}
		}
	}
	for id := range b.resting {
		if _, known := b.knownIDs[id]; !known {
			t.Fatalf("resting order %s is not in known ids", id)
		}
	}
	if count != len(b.resting) {
		t.Fatalf("index has %d orders, levels have %d", len(b.resting), count)
	}
}

func TestRestsWhenBookEmpty(t *testing.T) {
	b := NewBook(sym)
	trades := mustAdd(t, b, &Order{ID: "b1", Seq: 1, Side: Buy, Price: 10000, Qty: 5})
	if len(trades) != 0 {
		t.Fatalf("expected no trades, got %+v", trades)
	}
	if p, ok := b.BestBid(); !ok || p != 10000 {
		t.Fatalf("best bid = %d, %v", p, ok)
	}
}

func TestFullFillEmptiesBook(t *testing.T) {
	b := NewBook(sym)
	mustAdd(t, b, &Order{ID: "s1", Seq: 1, Side: Sell, Price: 10000, Qty: 5})
	trades := mustAdd(t, b, &Order{ID: "b1", Seq: 2, Side: Buy, Price: 10000, Qty: 5})
	if len(trades) != 1 || trades[0].Qty != 5 {
		t.Fatalf("got %+v", trades)
	}
	if b.OrderCount() != 0 {
		t.Fatalf("book should be empty, has %d orders", b.OrderCount())
	}
}

func TestPriceImprovementUsesMakerPrice(t *testing.T) {
	b := NewBook(sym)
	mustAdd(t, b, &Order{ID: "s1", Seq: 1, Side: Sell, Price: 10000, Qty: 5})
	trades := mustAdd(t, b, &Order{ID: "b1", Seq: 2, Side: Buy, Price: 10100, Qty: 3})
	if len(trades) != 1 {
		t.Fatalf("expected 1 trade, got %d", len(trades))
	}
	got := trades[0]
	want := Trade{ID: "ACME-USD-2-1", Seq: 2, Price: 10000, Qty: 3, MakerID: "s1", TakerID: "b1", TakerSide: Buy}
	if got != want {
		t.Fatalf("got %+v\nwant %+v", got, want)
	}
}

func TestPartialFillOfMaker(t *testing.T) {
	b := NewBook(sym)
	mustAdd(t, b, &Order{ID: "s1", Seq: 1, Side: Sell, Price: 10000, Qty: 5})
	mustAdd(t, b, &Order{ID: "b1", Seq: 2, Side: Buy, Price: 10000, Qty: 3})
	_, asks := b.Depth(1)
	if len(asks) != 1 || asks[0].Qty != 2 {
		t.Fatalf("maker should have 2 left, depth %+v", asks)
	}
}

func TestPartialFillOfTakerRests(t *testing.T) {
	b := NewBook(sym)
	mustAdd(t, b, &Order{ID: "s1", Seq: 1, Side: Sell, Price: 10000, Qty: 2})
	mustAdd(t, b, &Order{ID: "b1", Seq: 2, Side: Buy, Price: 10000, Qty: 5})
	bids, asks := b.Depth(1)
	if len(asks) != 0 || len(bids) != 1 || bids[0].Qty != 3 || bids[0].Price != 10000 {
		t.Fatalf("bids %+v asks %+v", bids, asks)
	}
}

func TestSweepsSeveralLevelsAndStopsAtLimit(t *testing.T) {
	b := NewBook(sym)
	mustAdd(t, b, &Order{ID: "s1", Seq: 1, Side: Sell, Price: 10000, Qty: 1})
	mustAdd(t, b, &Order{ID: "s2", Seq: 2, Side: Sell, Price: 10100, Qty: 1})
	mustAdd(t, b, &Order{ID: "s3", Seq: 3, Side: Sell, Price: 10200, Qty: 1})
	trades := mustAdd(t, b, &Order{ID: "b1", Seq: 4, Side: Buy, Price: 10100, Qty: 10})

	if len(trades) != 2 || trades[0].Price != 10000 || trades[1].Price != 10100 {
		t.Fatalf("got %+v", trades)
	}
	if trades[1].ID != "ACME-USD-4-2" {
		t.Fatalf("second trade id = %s", trades[1].ID)
	}
	if ask, _ := b.BestAsk(); ask != 10200 {
		t.Fatalf("10200 must stay untouched, best ask = %d", ask)
	}
	if bid, _ := b.BestBid(); bid != 10100 {
		t.Fatalf("remainder 8 should rest at 10100, best bid = %d", bid)
	}
}

func TestTimePriorityOnSameLevel(t *testing.T) {
	b := NewBook(sym)
	mustAdd(t, b, &Order{ID: "s1", Seq: 1, Side: Sell, Price: 10000, Qty: 2})
	mustAdd(t, b, &Order{ID: "s2", Seq: 2, Side: Sell, Price: 10000, Qty: 2})
	trades := mustAdd(t, b, &Order{ID: "b1", Seq: 3, Side: Buy, Price: 10000, Qty: 3})
	if len(trades) != 2 || trades[0].MakerID != "s1" || trades[0].Qty != 2 || trades[1].MakerID != "s2" || trades[1].Qty != 1 {
		t.Fatalf("got %+v", trades)
	}
}

func TestNoCrossBothRest(t *testing.T) {
	b := NewBook(sym)
	mustAdd(t, b, &Order{ID: "s1", Seq: 1, Side: Sell, Price: 10000, Qty: 1})
	trades := mustAdd(t, b, &Order{ID: "b1", Seq: 2, Side: Buy, Price: 9900, Qty: 1})
	if len(trades) != 0 || b.OrderCount() != 2 {
		t.Fatalf("trades %+v, orders %d", trades, b.OrderCount())
	}
}

func TestSellTakerHitsBestBid(t *testing.T) {
	b := NewBook(sym)
	mustAdd(t, b, &Order{ID: "b1", Seq: 1, Side: Buy, Price: 9900, Qty: 1})
	mustAdd(t, b, &Order{ID: "b2", Seq: 2, Side: Buy, Price: 10000, Qty: 1})
	trades := mustAdd(t, b, &Order{ID: "s1", Seq: 3, Side: Sell, Price: 9800, Qty: 1})
	if len(trades) != 1 || trades[0].MakerID != "b2" || trades[0].Price != 10000 || trades[0].TakerSide != Sell {
		t.Fatalf("got %+v", trades)
	}
}

func TestCancel(t *testing.T) {
	b := NewBook(sym)
	mustAdd(t, b, &Order{ID: "s1", Seq: 1, Side: Sell, Price: 10000, Qty: 5})
	mustAdd(t, b, &Order{ID: "s2", Seq: 2, Side: Sell, Price: 10000, Qty: 1})
	mustAdd(t, b, &Order{ID: "b1", Seq: 3, Side: Buy, Price: 10000, Qty: 2})

	t.Run("existing returns remaining", func(t *testing.T) {
		rem, err := b.Cancel(4, "s1")
		if err != nil || rem != 3 {
			t.Fatalf("rem %d err %v", rem, err)
		}
		checkInvariants(t, b)
	})
	t.Run("missing", func(t *testing.T) {
		if _, err := b.Cancel(5, "nope"); !errors.Is(err, ErrOrderNotFound) {
			t.Fatalf("err %v", err)
		}
	})
	t.Run("already filled", func(t *testing.T) {
		mustAdd(t, b, &Order{ID: "b2", Seq: 6, Side: Buy, Price: 10000, Qty: 1})
		if _, err := b.Cancel(7, "s2"); !errors.Is(err, ErrOrderNotFound) {
			t.Fatalf("err %v", err)
		}
		if b.OrderCount() != 0 {
			t.Fatalf("book should be empty")
		}
	})
}

func TestValidation(t *testing.T) {
	cases := []struct {
		name string
		o    Order
		want error
	}{
		{"zero side", Order{ID: "x", Seq: 1, Price: 1, Qty: 1}, ErrInvalidSide},
		{"zero price", Order{ID: "x", Seq: 1, Side: Buy, Qty: 1}, ErrInvalidPrice},
		{"negative qty", Order{ID: "x", Seq: 1, Side: Buy, Price: 1, Qty: -1}, ErrInvalidQty},
		{"empty id", Order{Seq: 1, Side: Buy, Price: 1, Qty: 1}, ErrEmptyID},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b := NewBook(sym)
			if _, err := b.Add(&c.o); !errors.Is(err, c.want) {
				t.Fatalf("err %v, want %v", err, c.want)
			}
			if b.LastSeq() != 1 {
				t.Fatalf("rejected command must still consume its seq")
			}
		})
	}

	t.Run("seq must increase", func(t *testing.T) {
		b := NewBook(sym)
		mustAdd(t, b, &Order{ID: "a", Seq: 5, Side: Buy, Price: 1, Qty: 1})
		if _, err := b.Add(&Order{ID: "b", Seq: 5, Side: Buy, Price: 1, Qty: 1}); !errors.Is(err, ErrSeqNotIncreased) {
			t.Fatalf("err %v", err)
		}
	})
	t.Run("duplicate id", func(t *testing.T) {
		b := NewBook(sym)
		mustAdd(t, b, &Order{ID: "a", Seq: 1, Side: Buy, Price: 1, Qty: 1})
		if _, err := b.Add(&Order{ID: "a", Seq: 2, Side: Buy, Price: 1, Qty: 1}); !errors.Is(err, ErrDuplicateID) {
			t.Fatalf("err %v", err)
		}
	})
}

func TestSnapshotRoundTrip(t *testing.T) {
	b := NewBook(sym)
	mustAdd(t, b, &Order{ID: "s1", Seq: 1, Side: Sell, Price: 10100, Qty: 5})
	mustAdd(t, b, &Order{ID: "s2", Seq: 2, Side: Sell, Price: 10100, Qty: 3})
	mustAdd(t, b, &Order{ID: "b1", Seq: 3, Side: Buy, Price: 9900, Qty: 4})
	mustAdd(t, b, &Order{ID: "b2", Seq: 4, Side: Buy, Price: 10100, Qty: 2})

	data, err := b.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	r, err := Restore(data)
	if err != nil {
		t.Fatal(err)
	}
	checkInvariants(t, r)
	if r.HashHex() != b.HashHex() || r.LastSeq() != 4 {
		t.Fatalf("restored book differs")
	}

	t1, _ := b.Add(&Order{ID: "b3", Seq: 5, Side: Buy, Price: 10100, Qty: 10})
	t2, _ := r.Add(&Order{ID: "b3", Seq: 5, Side: Buy, Price: 10100, Qty: 10})
	if len(t1) != len(t2) || t1[0] != t2[0] || r.HashHex() != b.HashHex() {
		t.Fatalf("diverged after restore")
	}
}

func TestDuplicateIDRejectedAfterFillAndCancel(t *testing.T) {
	b := NewBook(sym)
	mustAdd(t, b, &Order{ID: "s1", Seq: 1, Side: Sell, Price: 10000, Qty: 1})
	mustAdd(t, b, &Order{ID: "b1", Seq: 2, Side: Buy, Price: 10000, Qty: 1})
	if _, err := b.Add(&Order{ID: "s1", Seq: 3, Side: Sell, Price: 10000, Qty: 1}); !errors.Is(err, ErrDuplicateID) {
		t.Fatalf("filled order replayed as new: err %v", err)
	}

	mustAdd(t, b, &Order{ID: "s2", Seq: 4, Side: Sell, Price: 10000, Qty: 1})
	if _, err := b.Cancel(5, "s2"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Add(&Order{ID: "s2", Seq: 6, Side: Sell, Price: 10000, Qty: 1}); !errors.Is(err, ErrDuplicateID) {
		t.Fatalf("cancelled order replayed as new: err %v", err)
	}
	if b.OrderCount() != 0 || b.KnownOrderCount() != 3 {
		t.Fatalf("orders %d, known %d", b.OrderCount(), b.KnownOrderCount())
	}
}

func TestDepthLimit(t *testing.T) {
	b := NewBook(sym)
	mustAdd(t, b, &Order{ID: "b1", Seq: 1, Side: Buy, Price: 9900, Qty: 1})
	mustAdd(t, b, &Order{ID: "b2", Seq: 2, Side: Buy, Price: 9800, Qty: 2})
	for _, limit := range []int{-1, 0} {
		bids, asks := b.Depth(limit)
		if len(bids) != 0 || len(asks) != 0 {
			t.Fatalf("Depth(%d) = %v %v", limit, bids, asks)
		}
	}
	bids, _ := b.Depth(5)
	if len(bids) != 2 || bids[0].Price != 9900 || bids[1].Qty != 2 {
		t.Fatalf("Depth(5) bids = %+v", bids)
	}
}

func TestRestoreRejectsBrokenSnapshots(t *testing.T) {
	valid := func() snapshotData {
		return snapshotData{
			Version: Version, Symbol: sym, LastSeq: 3,
			Bids:     []snapshotLevel{{Price: 9900, Orders: []Order{{ID: "b1", Seq: 1, Side: Buy, Price: 9900, Qty: 5, Remaining: 5}}}},
			Asks:     []snapshotLevel{{Price: 10000, Orders: []Order{{ID: "s1", Seq: 2, Side: Sell, Price: 10000, Qty: 5, Remaining: 2}, {ID: "s2", Seq: 3, Side: Sell, Price: 10000, Qty: 1, Remaining: 1}}}},
			KnownIDs: []string{"b1", "s1", "s2"},
		}
	}
	encode := func(t *testing.T, data snapshotData) []byte {
		t.Helper()
		raw, err := json.Marshal(data)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}

	restored, err := Restore(encode(t, valid()))
	if err != nil {
		t.Fatalf("valid snapshot rejected: %v", err)
	}
	checkInvariants(t, restored)

	cases := []struct {
		name   string
		mutate func(*snapshotData)
	}{
		{"wrong version", func(s *snapshotData) { s.Version = "book/v1" }},
		{"empty symbol", func(s *snapshotData) { s.Symbol = "" }},
		{"crossed book", func(s *snapshotData) {
			s.Bids[0].Price, s.Bids[0].Orders[0].Price = 10000, 10000
		}},
		{"order on wrong side", func(s *snapshotData) { s.Bids[0].Orders[0].Side = Sell }},
		{"price differs from level", func(s *snapshotData) { s.Asks[0].Orders[0].Price = 10100 }},
		{"remaining above qty", func(s *snapshotData) { s.Asks[0].Orders[0].Remaining = 6 }},
		{"zero remaining", func(s *snapshotData) { s.Asks[0].Orders[0].Remaining = 0 }},
		{"seq after last", func(s *snapshotData) { s.Asks[0].Orders[1].Seq = 4 }},
		{"fifo broken", func(s *snapshotData) { s.Asks[0].Orders[0].Seq, s.Asks[0].Orders[1].Seq = 3, 2 }},
		{"duplicate order", func(s *snapshotData) { s.Asks[0].Orders[1].ID = "s1" }},
		{"empty level", func(s *snapshotData) { s.Bids[0].Orders = nil }},
		{"unsorted levels", func(s *snapshotData) {
			s.Bids = append(s.Bids, snapshotLevel{Price: 9950, Orders: []Order{{ID: "b2", Seq: 1, Side: Buy, Price: 9950, Qty: 1, Remaining: 1}}})
			s.KnownIDs = []string{"b1", "b2", "s1", "s2"}
		}},
		{"resting order not known", func(s *snapshotData) { s.KnownIDs = []string{"b1", "s1"} }},
		{"known ids unsorted", func(s *snapshotData) { s.KnownIDs = []string{"s1", "b1", "s2"} }},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			data := valid()
			testCase.mutate(&data)
			if _, err := Restore(encode(t, data)); !errors.Is(err, ErrBadSnapshot) {
				t.Fatalf("err %v, want ErrBadSnapshot", err)
			}
		})
	}

	if _, err := Restore([]byte("{not json")); !errors.Is(err, ErrBadSnapshot) {
		t.Fatalf("garbage accepted: %v", err)
	}
}
