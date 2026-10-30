package book

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
)

const Version = "book/v1"

type snapshotLevel struct {
	Price  int64   `json:"p"`
	Orders []Order `json:"o"`
}

type snapshot struct {
	Version string          `json:"v"`
	Symbol  string          `json:"s"`
	LastSeq uint64          `json:"seq"`
	Bids    []snapshotLevel `json:"b"`
	Asks    []snapshotLevel `json:"a"`
}

func dumpLevels(src []*level) []snapshotLevel {
	out := make([]snapshotLevel, len(src))
	for i, l := range src {
		orders := make([]Order, len(l.orders))
		for j, o := range l.orders {
			orders[j] = *o
		}
		out[i] = snapshotLevel{
			Price:  l.price,
			Orders: orders,
		}
	}
	return out
}

func (b *Book) Snapshot() ([]byte, error) {
	return json.Marshal(snapshot{
		Version: Version,
		Symbol:  b.symbol,
		LastSeq: b.lastSeq,
		Bids:    dumpLevels(b.bids),
		Asks:    dumpLevels(b.asks),
	})
}

func (b *Book) Hash() ([32]byte, error) {
	data, err := b.Snapshot()
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(data), nil
}

func (b *Book) HashHex() string {
	h, err := b.Hash()
	if err != nil {
		return "error:" + err.Error()
	}
	return fmt.Sprintf("%x", h[:])
}

func Restore(data []byte) (*Book, error) {
	var s snapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("book: decode snapshot: %w", err)
	}
	if s.Version != Version {
		return nil, fmt.Errorf(
			"book: snapshot version %q, engine is %q",
			s.Version,
			Version,
		)
	}

	b := NewBook(s.Symbol)
	b.lastSeq = s.LastSeq

	load := func(src []snapshotLevel) []*level {
		out := make([]*level, len(src))
		for i, sl := range src {
			l := &level{
				price:  sl.Price,
				orders: make([]*Order, len(sl.Orders)),
			}
			for j := range sl.Orders {
				o := sl.Orders[j]
				l.orders[j] = &o
				b.index[o.ID] = &o
			}
			out[i] = l
		}
		return out
	}

	b.bids = load(s.Bids)
	b.asks = load(s.Asks)

	return b, nil
}
