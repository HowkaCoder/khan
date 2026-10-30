package events

import "fmt"

type Command struct {
	Type    string `json:"type"`
	OrderID string `json:"order_id"`
	Side    string `json:"side,omitempty"`
	Price   int64  `json:"price,omitempty"`
	Qty     int64  `json:"qty,omitempty"`
}

const (
	CmdNew    = "new"
	CmdCancel = "cancel"
)

type Exec struct {
	Type   string `json:"type"`
	ID     string `json:"id"`
	Symbol string `json:"symbol"`
	Seq    uint64 `json:"seq"`

	Price        int64  `json:"price,omitempty"`
	Qty          int64  `json:"qty,omitempty"`
	MakerOrderID string `json:"maker_order_id,omitempty"`
	TakerOrderID string `json:"taker_order_id,omitempty"`
	TakerSide    string `json:"taker_side,omitempty"`

	OrderID   string `json:"order_id,omitempty"`
	Remaining int64  `json:"remaining,omitempty"`
	Reason    string `json:"reason,omitempty"`
}

const (
	ExecTrade     = "trade"
	ExecAccepted  = "accepted"
	ExecRejected  = "rejected"
	ExecCancelled = "cancelled"
)

const (
	StreamOrders = "ORDERS"
	StreamTrades = "TRADES"
	StreamDLQ    = "DLQ"
)

func OrdersSubject(symbol string) string { return "orders." + symbol }
func TradesSubject(symbol string) string { return "trades." + symbol }

func AcceptedID(symbol string, seq uint64) string  { return fmt.Sprintf("%s-%d-acc", symbol, seq) }
func RejectedID(symbol string, seq uint64) string  { return fmt.Sprintf("%s-%d-rej", symbol, seq) }
func CancelledID(symbol string, seq uint64) string { return fmt.Sprintf("%s-%d-cxl", symbol, seq) }

func NewOrderMsgID(orderID string) string    { return "new-" + orderID }
func CancelOrderMsgID(orderID string) string { return "cxl-" + orderID }
