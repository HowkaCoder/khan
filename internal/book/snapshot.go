package book

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
)

const Version = "book/v2"

var ErrBadSnapshot = errors.New("снапшот стакана повреждён или несовместим")

type snapshotLevel struct {
	Price  int64   `json:"p"`
	Orders []Order `json:"o"`
}

type snapshotData struct {
	Version  string          `json:"v"`
	Symbol   string          `json:"s"`
	LastSeq  uint64          `json:"seq"`
	Bids     []snapshotLevel `json:"b"`
	Asks     []snapshotLevel `json:"a"`
	KnownIDs []string        `json:"known"`
}

func (b *Book) Snapshot() ([]byte, error) {
	data := snapshotData{
		Version:  Version,
		Symbol:   b.symbol,
		LastSeq:  b.lastSeq,
		Bids:     copyLevels(b.bids),
		Asks:     copyLevels(b.asks),
		KnownIDs: slices.Sorted(maps.Keys(b.knownIDs)),
	}
	return json.Marshal(data)
}

func copyLevels(levels []*level) []snapshotLevel {
	result := make([]snapshotLevel, len(levels))
	for i, priceLevel := range levels {
		orders := make([]Order, len(priceLevel.orders))
		for j, order := range priceLevel.orders {
			orders[j] = *order
		}
		result[i] = snapshotLevel{Price: priceLevel.price, Orders: orders}
	}
	return result
}

func (b *Book) Hash() ([32]byte, error) {
	data, err := b.Snapshot()
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(data), nil
}

func (b *Book) HashHex() string {
	hash, err := b.Hash()
	if err != nil {
		return "error:" + err.Error()
	}
	return hex.EncodeToString(hash[:])
}

func Restore(raw []byte) (*Book, error) {
	var data snapshotData
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, fmt.Errorf("%w: не удалось разобрать JSON: %w", ErrBadSnapshot, err)
	}
	if data.Version != Version {
		return nil, fmt.Errorf("%w: версия %q, движок ожидает %q", ErrBadSnapshot, data.Version, Version)
	}
	if data.Symbol == "" {
		return nil, fmt.Errorf("%w: пустое имя торговой пары", ErrBadSnapshot)
	}

	restored := NewBook(data.Symbol)
	restored.lastSeq = data.LastSeq

	if err := restored.loadKnownIDs(data.KnownIDs); err != nil {
		return nil, err
	}
	bids, err := restored.loadSide(Buy, data.Bids)
	if err != nil {
		return nil, err
	}
	asks, err := restored.loadSide(Sell, data.Asks)
	if err != nil {
		return nil, err
	}
	restored.bids = bids
	restored.asks = asks

	bestBid, hasBid := restored.BestBid()
	bestAsk, hasAsk := restored.BestAsk()
	if hasBid && hasAsk && bestBid >= bestAsk {
		return nil, fmt.Errorf("%w: лучшая покупка %d не ниже лучшей продажи %d", ErrBadSnapshot, bestBid, bestAsk)
	}
	return restored, nil
}

func (b *Book) loadKnownIDs(ids []string) error {
	for i, id := range ids {
		if id == "" {
			return fmt.Errorf("%w: пустой идентификатор в списке известных заявок", ErrBadSnapshot)
		}
		if i > 0 && ids[i-1] >= id {
			return fmt.Errorf("%w: список известных заявок не отсортирован или содержит повтор", ErrBadSnapshot)
		}
		b.knownIDs[id] = struct{}{}
	}
	return nil
}

func (b *Book) loadSide(side Side, levels []snapshotLevel) ([]*level, error) {
	result := make([]*level, 0, len(levels))
	for i, savedLevel := range levels {
		if savedLevel.Price <= 0 {
			return nil, fmt.Errorf("%w: уровень с ценой %d", ErrBadSnapshot, savedLevel.Price)
		}
		if i > 0 && !isBetterPrice(side, levels[i-1].Price, savedLevel.Price) {
			return nil, fmt.Errorf("%w: уровни %s не отсортированы", ErrBadSnapshot, side)
		}
		if len(savedLevel.Orders) == 0 {
			return nil, fmt.Errorf("%w: пустой уровень %d", ErrBadSnapshot, savedLevel.Price)
		}

		priceLevel := &level{price: savedLevel.Price, orders: make([]*Order, 0, len(savedLevel.Orders))}
		var previousSeq uint64
		for _, savedOrder := range savedLevel.Orders {
			order := savedOrder
			if err := b.checkRestoredOrder(&order, side, savedLevel.Price, previousSeq); err != nil {
				return nil, err
			}
			previousSeq = order.Seq
			priceLevel.orders = append(priceLevel.orders, &order)
			b.resting[order.ID] = &order
		}
		result = append(result, priceLevel)
	}
	return result, nil
}

func (b *Book) checkRestoredOrder(order *Order, side Side, levelPrice int64, previousSeq uint64) error {
	switch {
	case order.ID == "":
		return fmt.Errorf("%w: заявка без идентификатора", ErrBadSnapshot)
	case order.Side != side:
		return fmt.Errorf("%w: заявка %s стоит не на своей стороне", ErrBadSnapshot, order.ID)
	case order.Price != levelPrice:
		return fmt.Errorf("%w: цена заявки %s не совпадает с уровнем", ErrBadSnapshot, order.ID)
	case order.Qty <= 0 || order.Remaining <= 0 || order.Remaining > order.Qty:
		return fmt.Errorf("%w: у заявки %s неверный остаток", ErrBadSnapshot, order.ID)
	case order.Seq == 0 || order.Seq > b.lastSeq:
		return fmt.Errorf("%w: у заявки %s неверный seq %d", ErrBadSnapshot, order.ID, order.Seq)
	case order.Seq <= previousSeq:
		return fmt.Errorf("%w: нарушен порядок FIFO на уровне %d", ErrBadSnapshot, levelPrice)
	}
	if _, duplicate := b.resting[order.ID]; duplicate {
		return fmt.Errorf("%w: заявка %s встречается дважды", ErrBadSnapshot, order.ID)
	}
	if _, known := b.knownIDs[order.ID]; !known {
		return fmt.Errorf("%w: заявки %s нет в списке известных", ErrBadSnapshot, order.ID)
	}
	return nil
}
