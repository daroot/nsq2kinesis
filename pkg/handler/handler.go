package handler

import (
	"github.com/daroot/nsq2kinesis/pkg/deduper"
	nsq "github.com/nsqio/go-nsq"
)

// BatchingHandler processes NSQ messages asynchronously.  It deduplicates
// incoming messages, disables the NSQ handler auto-response, and then passes
// each message down a pipe to something which is going to have logic for
// batching and then doing something with the message.
type BatchingHandler struct {
	inflight chan<- *nsq.Message
	seen     *deduper.Deduper
}

// HandleMessage satisifies nsq.Handler interface.
func (d *BatchingHandler) HandleMessage(m *nsq.Message) error {
	m.DisableAutoResponse()
	if d.seen.Test(m.Body) {
		m.Finish()
		return nil
	}
	d.inflight <- m
	return nil
}

// RotateDeduper issues a Trim to the internal deduper, so that the
// deduplication cache does not grow without bounds.  This should be called
// periodically.  The deduper is concurrency safe, so this can be done
// from an independent goroutine.
func (d *BatchingHandler) RotateDeduper() {
	d.seen.Trim()
}

// NewBatchingHandler creates a new BatchingHandler which will feed incoming
// messages to the provided channel.
func NewBatchingHandler(c chan<- *nsq.Message) *BatchingHandler {
	return &BatchingHandler{
		inflight: c,
		seen:     deduper.New(),
	}
}
