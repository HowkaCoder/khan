package book

import (
	"errors"
	"fmt"
	"slices"
	"sort"
)

type Side uint8

const (
	SideUnknown Side = iota
	Buy
	Sell
)

func (side Side) String() string {
	switch side {
	case Buy:
		return "buy"
	case Sell:
		return "sell"
	default:
		return "unknown"
	}
}

func ParseSide(text string) (Side, error) {
	switch text {
	case "buy":
		return Buy, nil
	case "sell":
		return Sell, nil
	default:
		return SideUnknown, ErrInvalidSide
	}
}

type Order struct {
	ID        string `json:"id"`
	Seq       uint64 `json:"seq"`
	Side      Side   `json:"side"`
	Price     int64  `json:"price"`
	Qty       int64  `json:"qty"`
	Remaining int64  `json:"rem"`
}

type Trade struct {
	ID        string
	Seq       uint64
	Price     int64
	Qty       int64
	MakerID   string
	TakerID   string
	TakerSide Side
}

var (
	ErrInvalidSide     = errors.New("сторона заявки должна быть buy или sell")
	ErrInvalidPrice    = errors.New("цена должна быть больше нуля")
	ErrInvalidQty      = errors.New("количество должно быть больше нуля")
	ErrEmptyID         = errors.New("у заявки нет идентификатора")
	ErrSeqNotIncreased = errors.New("номер команды должен расти")
	ErrDuplicateID     = errors.New("заявка с таким идентификатором уже была")
	ErrOrderNotFound   = errors.New("заявка не найдена в стакане")
)

type level struct {
	price  int64
	orders []*Order
}

type Book struct {
	symbol   string
	bids     []*level
	asks     []*level
	resting  map[string]*Order
	knownIDs map[string]struct{}
	lastSeq  uint64
}

func NewBook(symbol string) *Book {
	return &Book{
		symbol:   symbol,
		resting:  make(map[string]*Order),
		knownIDs: make(map[string]struct{}),
	}
}

func (b *Book) Symbol() string  { return b.symbol }
func (b *Book) LastSeq() uint64 { return b.lastSeq }

func (b *Book) OrderCount() int { return len(b.resting) }

func (b *Book) KnownOrderCount() int { return len(b.knownIDs) }

func (b *Book) advance(seq uint64) error {
	if seq <= b.lastSeq {
		return fmt.Errorf("%w: получен %d, последний %d", ErrSeqNotIncreased, seq, b.lastSeq)
	}
	b.lastSeq = seq
	return nil
}

func (b *Book) Skip(seq uint64) error {
	return b.advance(seq)
}

func (b *Book) Add(order *Order) ([]Trade, error) {
	if err := b.advance(order.Seq); err != nil {
		return nil, err
	}
	if err := b.validate(order); err != nil {
		return nil, err
	}

	b.knownIDs[order.ID] = struct{}{}
	order.Remaining = order.Qty

	trades := b.match(order)
	if order.Remaining > 0 {
		b.rest(order)
	}
	return trades, nil
}

func (b *Book) validate(order *Order) error {
	if order.ID == "" {
		return ErrEmptyID
	}
	if order.Side != Buy && order.Side != Sell {
		return ErrInvalidSide
	}
	if order.Price <= 0 {
		return ErrInvalidPrice
	}
	if order.Qty <= 0 {
		return ErrInvalidQty
	}
	if _, seen := b.knownIDs[order.ID]; seen {
		return ErrDuplicateID
	}
	return nil
}

func (b *Book) match(taker *Order) []Trade {
	makerSide := opposite(taker.Side)
	var trades []Trade

	for taker.Remaining > 0 {
		levels := b.levels(makerSide)
		if len(levels) == 0 {
			break
		}
		bestLevel := levels[0]
		if !priceCrosses(taker, bestLevel.price) {
			break
		}

		maker := bestLevel.orders[0]
		quantity := min(taker.Remaining, maker.Remaining)
		tradeNumber := len(trades) + 1

		trades = append(trades, Trade{
			ID:        fmt.Sprintf("%s-%d-%d", b.symbol, taker.Seq, tradeNumber),
			Seq:       taker.Seq,
			Price:     bestLevel.price,
			Qty:       quantity,
			MakerID:   maker.ID,
			TakerID:   taker.ID,
			TakerSide: taker.Side,
		})

		taker.Remaining -= quantity
		maker.Remaining -= quantity

		if maker.Remaining == 0 {
			b.removeFirstOrderOfBestLevel(makerSide)
		}
	}
	return trades
}

func (b *Book) removeFirstOrderOfBestLevel(side Side) {
	levels := b.levels(side)
	bestLevel := levels[0]
	filled := bestLevel.orders[0]

	bestLevel.orders[0] = nil
	bestLevel.orders = bestLevel.orders[1:]
	delete(b.resting, filled.ID)

	if len(bestLevel.orders) == 0 {
		levels[0] = nil
		b.setLevels(side, levels[1:])
	}
}

func (b *Book) rest(order *Order) {
	levels := b.levels(order.Side)
	index, found := findLevel(levels, order.Side, order.Price)

	if found {
		levels[index].orders = append(levels[index].orders, order)
	} else {
		newLevel := &level{price: order.Price, orders: []*Order{order}}
		b.setLevels(order.Side, slices.Insert(levels, index, newLevel))
	}
	b.resting[order.ID] = order
}

func (b *Book) Cancel(seq uint64, orderID string) (int64, error) {
	if err := b.advance(seq); err != nil {
		return 0, err
	}
	order, ok := b.resting[orderID]
	if !ok {
		return 0, ErrOrderNotFound
	}

	levels := b.levels(order.Side)
	index, found := findLevel(levels, order.Side, order.Price)
	if found {
		priceLevel := levels[index]
		priceLevel.orders = slices.DeleteFunc(priceLevel.orders, func(candidate *Order) bool {
			return candidate == order
		})
		if len(priceLevel.orders) == 0 {
			b.setLevels(order.Side, slices.Delete(levels, index, index+1))
		}
	}
	delete(b.resting, orderID)

	remaining := order.Remaining
	order.Remaining = 0
	return remaining, nil
}

func (b *Book) BestBid() (int64, bool) {
	if len(b.bids) == 0 {
		return 0, false
	}
	return b.bids[0].price, true
}

func (b *Book) BestAsk() (int64, bool) {
	if len(b.asks) == 0 {
		return 0, false
	}
	return b.asks[0].price, true
}

type Level struct {
	Price  int64 `json:"price"`
	Qty    int64 `json:"qty"`
	Orders int   `json:"orders"`
}

func (b *Book) Depth(limit int) (bids, asks []Level) {
	return summarizeLevels(b.bids, limit), summarizeLevels(b.asks, limit)
}

func summarizeLevels(levels []*level, limit int) []Level {
	count := max(0, min(limit, len(levels)))
	result := make([]Level, 0, count)
	for _, priceLevel := range levels[:count] {
		var quantity int64
		for _, order := range priceLevel.orders {
			quantity += order.Remaining
		}
		result = append(result, Level{Price: priceLevel.price, Qty: quantity, Orders: len(priceLevel.orders)})
	}
	return result
}

func (b *Book) levels(side Side) []*level {
	if side == Buy {
		return b.bids
	}
	return b.asks
}

func (b *Book) setLevels(side Side, levels []*level) {
	if side == Buy {
		b.bids = levels
	} else {
		b.asks = levels
	}
}

func opposite(side Side) Side {
	if side == Buy {
		return Sell
	}
	return Buy
}

func priceCrosses(taker *Order, makerPrice int64) bool {
	if taker.Side == Buy {
		return makerPrice <= taker.Price
	}
	return makerPrice >= taker.Price
}

func isBetterPrice(side Side, price, than int64) bool {
	if side == Buy {
		return price > than
	}
	return price < than
}

func findLevel(levels []*level, side Side, price int64) (int, bool) {
	index := sort.Search(len(levels), func(i int) bool {
		return !isBetterPrice(side, levels[i].price, price)
	})
	found := index < len(levels) && levels[index].price == price
	return index, found
}
