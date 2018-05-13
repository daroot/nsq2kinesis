package handler

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	nsq "github.com/nsqio/go-nsq"
)

type NoOpMessageDelegate struct{}

func (d *NoOpMessageDelegate) OnFinish(m *nsq.Message) {
	// Gross, but we don't have internal access to m.responded here.
	m.Timestamp = 1
}
func (d *NoOpMessageDelegate) OnRequeue(m *nsq.Message, delay time.Duration, backoff bool) {}
func (d *NoOpMessageDelegate) OnTouch(m *nsq.Message)                                      {}

func makeMsg(c rune) *nsq.Message {
	var id [16]byte
	copy(id[0:16], "1234567890abcdef")
	m := nsq.NewMessage(id, []byte{byte(c)})
	m.Delegate = &NoOpMessageDelegate{}
	return m
}

type SyncBuffer struct {
	b bytes.Buffer
	m sync.Mutex
}

func (b *SyncBuffer) Write(p []byte) (n int, err error) {
	b.m.Lock()
	defer b.m.Unlock()
	return b.b.Write(p)
}

func (b *SyncBuffer) String() string {
	b.m.Lock()
	defer b.m.Unlock()
	return b.b.String()
}

func (b *SyncBuffer) Reset() {
	b.m.Lock()
	defer b.m.Unlock()
	b.b.Reset()
}

// Ensure our BatchingHandler correctly passes messages into the channel,
// Finishes() dupes, and disables auto responses.
func TestBatchingHandler(t *testing.T) {
	testCases := map[string]struct {
		ops      string
		result   string
		finished []bool
	}{
		"Nothing":             {"", "", []bool{}},
		"Single":              {"a", "a", []bool{false}},
		"Double":              {"ab", "ab", []bool{false, false}},
		"Dupe":                {"aa", "a", []bool{false, true}},
		"Dupe after 1 rotate": {"a#a", "a", []bool{false, true}},
		"Dupe after 2 Rotate": {"a##a", "aa", []bool{false, false}},
	}

	c := make(chan *nsq.Message)
	buff := SyncBuffer{}
	msgs := make([]*nsq.Message, 64)
	lock := sync.Mutex{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		for {
			select {
			case m := <-c:
				buff.Write(m.Body)
				lock.Lock()
				msgs = append(msgs, m)
				lock.Unlock()
			case <-ctx.Done():
				return
			}
		}
	}()

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			buff.Reset()
			lock.Lock()
			msgs = msgs[:0]
			lock.Unlock()
			bh := NewBatchingHandler(c)
			finished := []bool{}

			for i, ch := range tc.ops {
				switch ch {
				case '#':
					bh.RotateDeduper()
				default:
					msg := makeMsg(ch)
					bh.HandleMessage(msg)
					if !msg.IsAutoResponseDisabled() {
						t.Errorf("%s: msg %d not auto-response disabled", name, i)
					}
					finished = append(finished, msg.Timestamp == 1)
				}
			}

			// Need to let go routine reading channel do its thing.
			time.Sleep(time.Microsecond * 100)

			if buff.String() != tc.result {
				t.Errorf("%s: got %s, want %s", name, buff.String(), tc.result)
			}
			if len(finished) != len(tc.finished) {
				t.Errorf("%s: got %d msgs, want %d", name, len(finished), len(tc.finished))
			}
			for i := range finished {
				if finished[i] != tc.finished[i] {
					t.Errorf("%s: message at index %d, want %v, got %v", name, i, finished[i], tc.finished[i])
				}
			}
		})
	}
}

func BenchmarkHandler(b *testing.B) {
	c := make(chan *nsq.Message)
	msgs := make([]*nsq.Message, 64)
	for j := 0; j < 64; j++ {
		msgs[j] = makeMsg(rune('A' + j))
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	buf := bytes.Buffer{}
	go func() {
		for {
			select {
			case m := <-c:
				buf.Write(m.Body)
			case <-ctx.Done():
				return
			}
		}
	}()

	bh := NewBatchingHandler(c)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := bh.HandleMessage(msgs[i%64])
		if err != nil {
			b.Fatal(err)
		}
	}
}
