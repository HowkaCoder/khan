package book

import (
	"reflect"
	"testing"
)

func TestSnapshotRoundTrip(t *testing.T) {
	b := NewBook(sym)

	mustAdd(t, b, &Order{
		ID: "s1", Seq: 1, Side: Sell, Price: 100, Qty: 5,
	})
	mustAdd(t, b, &Order{
		ID: "s2", Seq: 2, Side: Sell, Price: 100, Qty: 3,
	})
	mustAdd(t, b, &Order{
		ID: "b1", Seq: 3, Side: Buy, Price: 99, Qty: 4,
	})
	mustAdd(t, b, &Order{
		ID: "b2", Seq: 4, Side: Buy, Price: 100, Qty: 2,
	})

	data, err := b.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	r, err := Restore(data)
	if err != nil {
		t.Fatal(err)
	}

	checkInvariants(t, r)

	if r.HashHex() != b.HashHex() {
		t.Fatal("состояние после восстановления отличается")
	}

	if r.Symbol() != sym || r.LastSeq() != 4 {
		t.Fatal("не восстановлены symbol или lastSeq")
	}

	if r.index["s1"] == b.index["s1"] {
		t.Fatal("восстановленный стакан использует исходную заявку")
	}

	next := Order{
		ID: "b3", Seq: 5, Side: Buy, Price: 100, Qty: 10,
	}
	copyNext := next

	originalTrades := mustAdd(t, b, &next)
	restoredTrades := mustAdd(t, r, &copyNext)

	if !reflect.DeepEqual(originalTrades, restoredTrades) {
		t.Fatalf(
			"сделки отличаются: original %+v, restored %+v",
			originalTrades,
			restoredTrades,
		)
	}

	if b.HashHex() != r.HashHex() {
		t.Fatal("стаканы разошлись после одинаковой команды")
	}
}

func TestRestoreRejectsInvalidData(t *testing.T) {
	if _, err := Restore([]byte("broken json")); err == nil {
		t.Fatal("ожидалась ошибка повреждённого JSON")
	}

	data := []byte(`{"v":"book/unsupported","s":"ACME-USD"}`)
	if _, err := Restore(data); err == nil {
		t.Fatal("ожидалась ошибка несовместимой версии")
	}
}
