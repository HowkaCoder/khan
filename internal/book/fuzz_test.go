package book

import (
	"fmt"
	"math/rand"
	"testing"
)

type cmd struct {
	cancel bool
	id     string
	side   Side
	price  int64
	qty    int64
}

func randomCommands(seed int64, count int) []cmd {
	random := rand.New(rand.NewSource(seed))
	commands := make([]cmd, 0, count)
	for i := 0; i < count; i++ {
		if i > 0 && random.Intn(5) == 0 {
			commands = append(commands, cmd{cancel: true, id: fmt.Sprintf("o%d", random.Intn(i))})
			continue
		}
		side := Buy
		if random.Intn(2) == 0 {
			side = Sell
		}
		commands = append(commands, cmd{
			id:    fmt.Sprintf("o%d", i),
			side:  side,
			price: 9900 + int64(random.Intn(21))*10,
			qty:   1 + int64(random.Intn(10)),
		})
	}
	return commands
}

func run(t testing.TB, commands []cmd) (*Book, []Trade) {
	t.Helper()
	b := NewBook(sym)
	var allTrades []Trade
	var added, cancelled, filled int64

	for i, command := range commands {
		seq := uint64(i + 1)
		if command.cancel {
			remaining, err := b.Cancel(seq, command.id)
			if err == nil {
				cancelled += remaining
			}
		} else {
			trades, err := b.Add(&Order{ID: command.id, Seq: seq, Side: command.side, Price: command.price, Qty: command.qty})
			if err != nil {
				t.Fatalf("command %d: %v", i, err)
			}
			added += command.qty
			for _, trade := range trades {
				filled += 2 * trade.Qty
			}
			allTrades = append(allTrades, trades...)
		}
		checkInvariants(t, b)
	}

	var resting int64
	bids, asks := b.Depth(1 << 30)
	for _, priceLevel := range append(bids, asks...) {
		resting += priceLevel.Qty
	}
	if added != filled+resting+cancelled {
		t.Fatalf("volume not conserved: added %d, filled %d, resting %d, cancelled %d", added, filled, resting, cancelled)
	}
	return b, allTrades
}

func TestDeterminism(t *testing.T) {
	commands := randomCommands(42, 5000)
	firstBook, firstTrades := run(t, commands)
	secondBook, secondTrades := run(t, commands)
	if firstBook.HashHex() != secondBook.HashHex() || len(firstTrades) != len(secondTrades) {
		t.Fatalf("same input, different result")
	}
	for i := range firstTrades {
		if firstTrades[i] != secondTrades[i] {
			t.Fatalf("trade %d differs: %+v vs %+v", i, firstTrades[i], secondTrades[i])
		}
	}
}

func TestReplayFromSnapshot(t *testing.T) {
	commands := randomCommands(7, 3000)
	fullBook, fullTrades := run(t, commands)

	headBook, _ := run(t, commands[:1500])
	snapshot, err := headBook.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := Restore(snapshot)
	if err != nil {
		t.Fatal(err)
	}

	var tailTrades []Trade
	for i := 1500; i < len(commands); i++ {
		command, seq := commands[i], uint64(i+1)
		if command.cancel {
			_, _ = restored.Cancel(seq, command.id)
			continue
		}
		trades, err := restored.Add(&Order{ID: command.id, Seq: seq, Side: command.side, Price: command.price, Qty: command.qty})
		if err != nil {
			t.Fatal(err)
		}
		tailTrades = append(tailTrades, trades...)
	}
	if restored.HashHex() != fullBook.HashHex() {
		t.Fatalf("hash after replay differs")
	}
	originalTail := fullTrades[len(fullTrades)-len(tailTrades):]
	for i := range tailTrades {
		if tailTrades[i] != originalTail[i] {
			t.Fatalf("trade %d differs after replay: %+v vs %+v", i, tailTrades[i], originalTail[i])
		}
	}
}

func FuzzBook(f *testing.F) {
	f.Add(int64(1), uint16(200))
	f.Add(int64(99), uint16(1000))
	f.Fuzz(func(t *testing.T, seed int64, count uint16) {
		b, _ := run(t, randomCommands(seed, int(count%2000)))
		snapshot, err := b.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		restored, err := Restore(snapshot)
		if err != nil {
			t.Fatalf("own snapshot rejected: %v", err)
		}
		if restored.HashHex() != b.HashHex() {
			t.Fatalf("hash changed after restore")
		}
	})
}

func FuzzRestore(f *testing.F) {
	seedBook, _ := run(f, randomCommands(5, 25))
	seedSnapshot, err := seedBook.Snapshot()
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seedSnapshot)
	f.Add([]byte(`{"v":"book/v2","s":"X","seq":0,"b":[],"a":[],"known":null}`))
	f.Add([]byte(`{}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		restored, err := Restore(raw)
		if err != nil {
			return
		}
		checkInvariants(t, restored)
		again, err := restored.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		second, err := Restore(again)
		if err != nil {
			t.Fatalf("snapshot of a restored book is rejected: %v", err)
		}
		if second.HashHex() != restored.HashHex() {
			t.Fatalf("hash changed after second restore")
		}
	})
}
