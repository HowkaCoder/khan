package book

import (
	"fmt"
	"testing"
)

func BenchmarkAdd(b *testing.B) {
	cmds := randomCommands(1, 100_000)
	ids := make([]string, len(cmds))
	for i := range cmds {
		ids[i] = fmt.Sprintf("o%d", i)
	}
	b.ReportAllocs()
	b.ResetTimer()

	bk := NewBook(sym)
	seq := uint64(0)
	for i := 0; i < b.N; i++ {
		j := i % len(cmds)
		if j == 0 {
			bk = NewBook(sym)
		}
		seq++
		c := cmds[j]
		if c.cancel {
			_, _ = bk.Cancel(seq, c.id)
			continue
		}
		_, _ = bk.Add(&Order{ID: ids[j], Seq: seq, Side: c.side, Price: c.price, Qty: c.qty})
	}
}
