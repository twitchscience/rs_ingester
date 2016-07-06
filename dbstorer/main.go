package main

import (
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/cactus/go-statsd-client/statsd"
	"github.com/twitchscience/aws_utils/listener"
	"github.com/twitchscience/rs_ingester/lib"
	"github.com/twitchscience/rs_ingester/metadata"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

var (
	pgConfig      metadata.PGConfig
	sqsPollWait   time.Duration
	sqsQueueName  string
	statsPrefix   string
	listenerCount int
)

type rdsPipeHandler struct {
	MetadataStorer metadata.Storer
	Signer         scoop_protocol.ScoopSigner
	Statter        statsd.Statter
}

func init() {
	flag.StringVar(&pgConfig.DatabaseURL, "databaseURL", "", "Postgres-scheme url for the RDS instance")
	flag.StringVar(&statsPrefix, "statsPrefix", "dbstorer", "the prefix to statsd")
	flag.IntVar(&pgConfig.MaxConnections, "maxDBConnections", 5, "Max number of database connections to open")
	flag.DurationVar(&sqsPollWait, "sqsPollWait", time.Second*30, "Number of seconds to wait between polling SQS")
	flag.StringVar(&sqsQueueName, "sqsQueueName", "", "Name of sqs queue to list for events on")
	flag.IntVar(&listenerCount, "listenerCount", 1, "Number of sqs listeners to run")
}

func main() {
	flag.Parse()

	log.SetOutput(os.Stdout)

	stats, err := lib.InitStats(statsPrefix)
	if err != nil {
		log.Fatalln("Error initializing stats:", err)
	}

	go func() {
		log.Println(http.ListenAndServe(":6061", nil))
	}()

	postgresBackend, err := metadata.NewPostgresStorer(&pgConfig)
	if err != nil {
		log.Fatalf("Error initializing PostgresStorer: %s", err)
	}

	session := session.New()
	// In cases we get a temporary influx of traffic, want to be resilient.
	sqs := sqs.New(session, aws.NewConfig().WithMaxRetries(10))

	listeners := make([]*listener.SQSListener, listenerCount)
	for i := 0; i < listenerCount; i++ {
		listeners[i] = startWorker(sqs, sqsQueueName, stats, postgresBackend)
	}

	wait := make(chan struct{})

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT)
	go func() {
		<-sigc
		// Cause flush
		var wg sync.WaitGroup
		wg.Add(listenerCount)
		for i := 0; i < listenerCount; i++ {
			go func() {
				defer wg.Done()
				listeners[i].Close()
			}()
		}
		wg.Wait()
		close(wait)
	}()

	<-wait
}

func startWorker(sqs sqsiface.SQSAPI, queue string, stats statsd.Statter, b metadata.Storer) *listener.SQSListener {
	ret := listener.BuildSQSListener(
		&rdsPipeHandler{
			MetadataStorer: b,
			Signer:         scoop_protocol.GetScoopSigner(),
			Statter:        stats,
		},
		sqsPollWait,
		sqs)
	go ret.Listen(queue)
	return ret
}

func (i *rdsPipeHandler) Handle(msg *sqs.Message) error {
	log.Printf("Got %s;%s\n", aws.StringValue(msg.Body), aws.StringValue(msg.MessageId))

	req, err := i.Signer.GetRowCopyRequest(strings.NewReader(aws.StringValue(msg.Body)))
	if err != nil {
		return err
	}

	load := metadata.Load(*req)
	err = i.MetadataStorer.InsertLoad(&load)
	if err != nil {
		return err
	}

	return nil
}
