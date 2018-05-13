package main

import (
	"context"
	"fmt"
	stdlog "log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/daroot/nsq2kinesis/pkg/handler"
	"github.com/namsral/flag"
	nsq "github.com/nsqio/go-nsq"
	"github.com/rs/zerolog"
)

var (
	version    = "dev"
	commit     = "none"
	date       = "unknown"
	nsqChannel = flag.String("channel", "nsq2kinesis", "channel to read from on nsqd")
	nsqTopic   = flag.String("topic", "", "topic to read from on nsqd")
	nsqdLoc    = flag.String("nsqd-tcp-address", "localhost:4150", "location of nsqd tcp endpoint")
	stream     = flag.String("stream", "", "Kinesis stream name")
	kinesisEnd = flag.String("kinesis-endpoint", "", "Kinesis endpoint")
	test       = flag.Bool("test", false, "make kinesis stream and use static creds (for testing)")
	showver    = flag.Bool("version", false, "show version and exit")
)

var parseOnce sync.Once

func main() {
	parseOnce.Do(flag.Parse)

	if *showver {
		fmt.Printf("nsq2kinesis %s (built %s, commit %s)\n", version, date, commit)
		os.Exit(0)
	}

	zerolog.TimeFieldFormat = "2006-01-02T15:04:05.999Z07:00"
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	time.Local = time.UTC
	log := zerolog.New(os.Stdout).With().Timestamp().Logger()
	// Make sure error logs from other libs go through zerolog.
	stdlog.SetOutput(log)
	stdlog.SetFlags(0)

	if *stream == "" || *nsqTopic == "" {
		fmt.Print("You must specify a Kinesis stream name and NSQ topic")
		os.Exit(-1)
	}

	ctx := context.Background()

	cfg := nsq.NewConfig()
	cfg.MaxInFlight = 1000                          // Not 1.
	cfg.OutputBufferSize = 64 * 1024                // Not 16k
	cfg.OutputBufferTimeout = time.Millisecond * 50 // not 250ms
	cfg.DefaultRequeueDelay = time.Second           // Not 90s
	cfg.MsgTimeout = time.Second * 10
	cfg.BackoffStrategy = &nsq.FullJitterStrategy{}
	cfg.UserAgent = "nsq2kinesis"

	c, err := nsq.NewConsumer(*nsqTopic, *nsqChannel, cfg)
	if err != nil {
		stdlog.Fatal("Unable to open nsq consumer", err)
	}

	msgCh := make(chan *nsq.Message)

	sess := session.Must(session.NewSession())

	awscfg := &aws.Config{}
	if *test {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
		if *kinesisEnd == "" {
			*kinesisEnd = fmt.Sprintf("kinesis.%s.amazonaws.com", *sess.Config.Region)
		}
		awscfg.Endpoint = kinesisEnd
		awscfg.Credentials = credentials.NewStaticCredentials("foo", "bar", "")
	}

	kclient := kinesis.New(sess, awscfg)
	if *test {
		res, err := kclient.CreateStream(&kinesis.CreateStreamInput{
			ShardCount: aws.Int64(1),
			StreamName: stream,
		})
		log.Debug().Interface("result", res).Err(err).Msg("stream creation")
		time.Sleep(time.Millisecond * 10)
	}

	bw := KinesisBatchWriter{
		Stream: *stream,
		Client: kclient,
		Log:    log,
	}

	go func(ctx context.Context, c <-chan *nsq.Message) {
		bw.Batch(ctx, c)
	}(ctx, msgCh)

	h := handler.NewBatchingHandler(msgCh)
	go func(ctx context.Context, h *handler.BatchingHandler) {
		for {
			select {
			case <-time.After(time.Second * 120):
				log.Info().Msg("rotating deduper")
				h.RotateDeduper()
			case <-ctx.Done():
				return
			}
		}
	}(ctx, h)

	c.AddConcurrentHandlers(h, 20)

	if err := c.ConnectToNSQD(*nsqdLoc); err != nil {
		stdlog.Fatal("unable to connect to nsqd", err)
	}

	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	log.Log().Msg("waiting for signal...")
	select {
	case <-c.StopChan:
		log.Log().Msg("Consumer stopped")
		return
	case <-sigCh:
		c.Stop()
		log.Log().Msg("Shutting down on signal")
	}
	time.Sleep(time.Millisecond * 20)
	os.Exit(0)
}
