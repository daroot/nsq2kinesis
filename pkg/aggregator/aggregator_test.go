package aggregator

import (
	"crypto/rand"
	"testing"

	deaggregation "github.com/kimutansk/go-kinesis-deaggregation"
)

func TestAggregator(t *testing.T) {
	agg := Aggregator{}

	t.Run("basic insert", func(t *testing.T) {
		i, _ := agg.Put([]byte("hello"))

		if agg.Count() != 1 {
			t.Error("expecting 1 record in aggregate, got", agg.Count())
		}

		// 21 = 5 bytes of "hello" + 16 byte partition key string
		if agg.Size() != 21 {
			t.Error("expecting size", 17, "got", agg.Size())
		}

		if agg.Recs() != 1 {
			t.Errorf("expecting only 1 rec got %d", agg.Recs())
		}

		if i != 0 {
			t.Errorf("expecting Put to return record 0, got %d", i)
		}
	})

	t.Run("additional insert", func(t *testing.T) {
		i, _ := agg.Put([]byte("world"))

		// 42 = 21 + 5 bytes of "world" + 16 more bytes of partition
		if agg.Size() != 42 {
			t.Error("expecting size", 43, "got", agg.Size())
		}

		if agg.Count() != 2 {
			t.Error("expecting 2 records in aggregate, got", agg.Count())
		}

		if agg.Recs() != 1 {
			t.Error("expecting still only 1 rec")
		}

		if i != 0 {
			t.Errorf("expecting Put to return record 0, got %d", i)
		}
	})

	t.Run(">1 PUT record because partition key", func(t *testing.T) {
		b := make([]byte, 24999)
		rand.Read(b)

		presize := agg.Size()
		i, _ := agg.Put(b, "rand")
		if agg.Size()-presize != 25003 {
			t.Errorf("expecting agg size to grow by %d, grew by %d", 25003, agg.Size()-presize)
		}

		if agg.Recs() != 2 {
			t.Errorf("expecting 2 recs, got %d", agg.Recs())
		}
		if i != 1 {
			t.Errorf("expecting record to be at slot 1, got %d", i)
		}
	})
	t.Run("Can drain records", func(t *testing.T) {
		i, _ := agg.Put([]byte("extra"))
		if i != 2 {
			t.Error("extra record not in slot 2, instead", i)
		}
		res, err := agg.Drain()
		if err != nil {
			t.Errorf("Unexpected error on drain %v", err)
		}

		// Hello+World, Large record, more+yet+more
		if len(res) != 3 {
			t.Errorf("Expecting 3 Kinesis records, got %d", len(res))
		}

		// 78 = 10 bytes of "hello"+"world" + 32 bytes of partition keys, + 20
		// bytes of AggregatedRecord wrapper (4 magic bytes + 16 bytes of md5 hash) +
		// 16 bytes of protobuf overhead.
		if len(res[0].Data) != 78 {
			t.Errorf("Expecting 78 byte data record, got %d", len(res[0].Data))
		}
	})

}

func TestDrainEmpties(t *testing.T) {
	agg := Aggregator{}
	agg.Put([]byte("here"))
	agg.Put([]byte("there"))
	agg.Put([]byte("back"))

	agg.Drain()

	if agg.Count() != 0 || agg.Size() != 0 || agg.Recs() != 0 {
		t.Errorf("Expected empty Aggregator, got size %d, count %d", agg.Size(), agg.Count())
	}
}

func TestEmptyDrainWorks(t *testing.T) {
	a := Aggregator{}
	res, err := a.Drain()
	if err != nil {
		t.Error("Expecting no error on empty drain", res, err)
	}
}

func TestOutputWorksWithDeaggregator(t *testing.T) {
	agg := Aggregator{}
	agg.Put([]byte("this"))
	agg.Put([]byte("that"))

	res, err := agg.Drain()
	if err != nil {
		t.Errorf("Unexpected error on drain %v", err)
	}

	parts, err := deaggregation.ExtractRecordDatas(res[0].Data)
	if err != nil {
		t.Errorf("deaggreation failed %v", err)
	}
	if len(parts) != 2 {
		t.Errorf("didn't get 2 user records, got %d", len(parts))
	}
	if string(parts[0]) != "this" || string(parts[1]) != "that" {
		t.Errorf("output records not equal to input records: %s, %s", string(parts[0]), string(parts[1]))
	}
}

func TestAggregateCrossesTarget(t *testing.T) {
	a := Aggregator{}
	b := make([]byte, 24994)
	rand.Read(b)
	a.Put([]byte("a"), "b")
	cntBefore := a.Recs()
	a.Put(b, "rand")
	if a.Recs()-cntBefore != 0 {
		t.Errorf("Unexpected new record on insert of 24994, cnt: %d, size %d", a.Recs(), a.Size())
	}
	a.Put([]byte("one"), "more")
	if a.Recs()-cntBefore != 1 {
		t.Error("Expecting additional record")
	}
}

func TestMultiplePartitionTable(t *testing.T) {
	a := Aggregator{}
	a.Put([]byte("too"), "p1")
	a.Put([]byte("and"), "p1")
	a.Put([]byte("fro"), "p1")
	res, err := a.Drain()
	if err != nil {
		t.Errorf("Unexpected error on drain %v", err)
	}

	arec, err := deaggregation.Unmarshal(res[0].Data)
	if err != nil {
		t.Errorf("unmarshal failed %v", err)
	}
	if len(arec.PartitionKeyTable) != 1 {
		t.Errorf("Partition table has wrong number of elements: %d, expecting 1", len(arec.PartitionKeyTable))
	}
}

func TestSingleLargePut(t *testing.T) {
	a := Aggregator{}
	b := make([]byte, 26000)
	rand.Read(b)
	presize := a.Size()
	prerecs := a.Recs()
	a.Put(b, "big")
	if a.Size()-presize != 26003 {
		t.Errorf("unespected size %d, (from %d), wanted 26003, %v, %v", a.Size()-presize, presize, a, a.completedRecs[0])
	}
	if a.Recs()-prerecs != 1 {
		t.Errorf("unexpected record count %d, wanted 1, %v, %v", a.Recs()-prerecs, a, a.completedRecs[0])
	}
}

func BenchmarkPut(b *testing.B) {
	rec := make([]byte, 0, 1024)
	rand.Read(rec)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		agg := Aggregator{}
		agg.Put(rec)
	}
}

func BenchmarkPut1000(b *testing.B) {
	recs := make([][]byte, 0, 1000)
	for i := 0; i < 1024; i++ {
		rec := make([]byte, 1000)
		s, _ := rand.Read(rec)
		if s != 1000 {
			b.Fatalf("didn't read 1000 from Rand, iter %d, size %d", i, s)
		}
		recs = append(recs, rec)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		agg := Aggregator{}
		for j := 0; j < 1000; j++ {
			agg.Put(recs[j], "a")
		}
		if agg.Count() != 1000 {
			b.Errorf("unexpected user record count %d", agg.Count())
		}
		if agg.Recs() != 42 {
			b.Errorf("unexpected aggregate record count %d, size: %d", agg.Recs(), agg.Size())
		}
	}
}

func BenchmarkDrain(b *testing.B) {
	recs := make([][]byte, 0, 1000)
	for i := 0; i < 1024; i++ {
		rec := make([]byte, 1000)
		rand.Read(rec)
		recs = append(recs, rec)
	}

	agg := Aggregator{}
	for j := 0; j < 100; j++ {
		agg.Put(recs[j], "a")
	}
	if agg.Count() != 100 {
		b.Errorf("unexpected user record count %d", agg.Count())
	}
	if agg.Recs() != 5 {
		b.Errorf("unexpected aggregate record count %d, size: %d", agg.Recs(), agg.Size())
	}
	for i := 0; i < b.N; i++ {
		test := agg
		o, _ := test.Drain()
		if len(o) != 5 {
			b.Log("not 5 completed records")
		}
	}
}
