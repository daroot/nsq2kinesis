package main

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	k "github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/daroot/nsq2kinesis/pkg/aggregator"
	nsq "github.com/nsqio/go-nsq"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
)

var (
	// ErrBatchTooBig is returned by Add if it would make the overall Kinesis
	// PutRecords batch too large.
	ErrBatchTooBig = errors.New("batch too big")
	// ErrNoStreamSpecified is returned by Send if Stream is not set.
	ErrNoStreamSpecified = errors.New("no stream specified")
)

// KinesisBatchWriter reads nsq.Messages from a channel until it's ready to
// send a single batch to the Kinesis endpoint.
type KinesisBatchWriter struct {
	MaxDelay time.Duration
	Stream   string
	Client   kinesisiface.KinesisAPI
	Log      zerolog.Logger

	msgs map[int][]*nsq.Message
	agg  aggregator.Aggregator
	once sync.Once
}

func (kbw *KinesisBatchWriter) init() {
	kbw.msgs = map[int][]*nsq.Message{}
	kbw.agg = aggregator.Aggregator{}
	if kbw.MaxDelay == 0 {
		kbw.MaxDelay = time.Second
	}
	if kbw.Client == nil {
		kbw.Client = k.New(session.Must(session.NewSession()))
	}
}

// Add inserts an NSQ message into a batch.  If the message would put the batch
// over size or record count limits, it returns ErrBatchTooBig.
func (kbw *KinesisBatchWriter) Add(m *nsq.Message) error {
	kbw.once.Do(kbw.init)

	// 4900000 not 5000000 to give padding for HTTP and Kinesis data structure
	// overhead
	if kbw.agg.Count()+1 >= 500 || kbw.agg.Size()+len(m.Body) >= 4900000 {
		return ErrBatchTooBig
	}

	recslot, err := kbw.agg.Put(m.Body)
	if err != nil {
		return err
	}

	// Since user records may be aggregated, multiple NSQ messages may need to
	// be Finish/Requeued when a single Kinesis record succeeds/fails.
	// Keep track of which nsq.Messages belong to which Kinesis record.
	if kbw.msgs[recslot] == nil {
		kbw.msgs[recslot] = []*nsq.Message{m}
	} else {
		kbw.msgs[recslot] = append(kbw.msgs[recslot], m)
	}

	return nil
}

// Send delivers a batch of records to Kinesis and acks all the nsq.Messages
// that went into it.
func (kbw *KinesisBatchWriter) Send(ctx context.Context) error {
	kbw.once.Do(kbw.init)

	recs, err := kbw.agg.Drain()
	if err != nil {
		return errors.Wrap(err, "draining aggregator")
	}

	if kbw.Stream == "" {
		return ErrNoStreamSpecified
	}

	kbw.Log.Debug().Int("count", len(recs)).Str("stream", kbw.Stream).Msg("preparing to send messages")
	out, err := kbw.Client.PutRecordsWithContext(ctx, &k.PutRecordsInput{
		Records:    recs,
		StreamName: &kbw.Stream,
	})
	if err != nil {
		kbw.Log.Error().Err(err).Msg("error sending to kinesis")
		if aerr, ok := err.(awserr.Error); ok {
			kbw.Log.Error().Str("code", aerr.Code()).Str("msg", aerr.Message()).Msg("was aws error")
			if aerr.Code() == k.ErrCodeProvisionedThroughputExceededException {
				// XXX: backoff and retry?
				kbw.Log.Debug().Msg("should backoff here.")
			}
		}
		return errors.Wrap(err, "failed records writing to kinesis")
	}
	kbw.Log.Info().Int("kinesis-records", len(recs)).Msg("successfully sent batch")

	// If the overall request succeded, mark the messages done.
	// Since we've stored the nsq.Message in a slice by aggregated slot, we
	// know which user records need to be Requeued if a particular Kinesis
	// record failed.
	for slot, rec := range out.Records {
		if rec.ErrorCode == nil && rec.ErrorMessage == nil {
			kbw.Log.Info().Int("messages", len(kbw.msgs[slot])).Int("slot", slot).Msg("finishing")
			for _, m := range kbw.msgs[slot] {
				m.Finish()
			}
		} else {
			kbw.Log.Info().Int("messages", len(kbw.msgs[slot])).Int("slot", slot).Msg("requeing")
			for _, m := range kbw.msgs[slot] {
				m.Requeue(-1)
			}
		}
	}
	kbw.msgs = map[int][]*nsq.Message{}

	return err
}

// Batch blocks, reading from a channel of *nsq.Message and determines when to
// send batches to Kinesis, based on record counts, size, and time.
func (kbw *KinesisBatchWriter) Batch(ctx context.Context, msgs <-chan *nsq.Message) error {
	kbw.once.Do(kbw.init)

	// This timer channel pattern is from
	// https://blog.gopheracademy.com/advent-2013/day-24-channel-buffering-patterns/
	// and avoids making an unnecessarily small batch if starting late in a
	// timeout cycle.  Instead, on every batch we reset our timer and don't
	// start it until the first message of a new batch comes in.
	timer := time.NewTimer(0)
	if !timer.Stop() {
		<-timer.C
	}

	var timerCh <-chan time.Time
	for {
		kbw.Log.Debug().Msg("starting select")
		select {
		case m, ok := <-msgs:
			kbw.Log.Debug().Interface("msg", m).Bool("timerch-on", timerCh != nil).Msg("msg received")
			if !ok {
				kbw.Log.Debug().Msg("msgs channel not okay, closing")
				msgs = nil
				// Upstream closed, do something with our current batch here.
				return kbw.Send(ctx)
			}
			// If this is the first message of a new batch, set our timer
			// channel up.
			if timerCh == nil {
				kbw.Log.Debug().Dur("timer", kbw.MaxDelay).Msg("batch timer started.")
				timerCh = timer.C
				timer.Reset(kbw.MaxDelay)
			}

			if len(m.Body) > 1024*1024 {
				kbw.Log.Warn().Int("size", len(m.Body)).Msg("message too large, ignoring")
				continue
			}

			if err := kbw.Add(m); err == ErrBatchTooBig {
				kbw.Log.Debug().Msg("Add ErrBatchTooBig")
				if err := kbw.Send(ctx); err != nil {
					kbw.Log.Error().Err(err).Msg("error while sending before large message")
					// XXX: Do something.
				}
				// Add as the first new record.
				kbw.Add(m)
				// New batch, so reset timer.
				timer.Reset(kbw.MaxDelay)
			} else if err != nil {
				// XXX: Something broke in Add.
				kbw.Log.Info().Msg("Add broke")
			} else {
				kbw.Log.Debug().Msg("Added msg to Aggregator")
			}
		case <-timerCh:
			kbw.Log.Debug().Msg("batch timeout.")
			// Timed out; send our current batch and reset our timer channel.
			if err := kbw.Send(ctx); err != nil {
				kbw.Log.Error().Err(err).Msg("kbw.send broke")
				// XXX: Do Something
				timer.Reset(kbw.MaxDelay)
			} else {
				timerCh = nil
			}
			kbw.Log.Debug().Bool("timerch-on", timerCh != nil).Msg("timer flipped?")
		case <-ctx.Done():
			// We got cancelled.  Send our current batch and exit.
			kbw.Send(ctx)
			return ctx.Err()
		}
	}
}
