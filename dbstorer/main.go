package main

import (
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/sqs"
	"github.com/cactus/go-statsd-client/statsd"
	"github.com/twitchscience/aws_utils/environment"
	"github.com/twitchscience/aws_utils/listener"
	"github.com/twitchscience/rs_ingester/lib"
	"github.com/twitchscience/rs_ingester/metadata"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

var (
	env         = environment.GetCloudEnv()
	pgConfig    metadata.PGConfig
	sqsPollWait time.Duration
	statsPrefix string
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
}

func main() {
	flag.Parse()

	log.SetOutput(os.Stdout)
	auth, err := aws.GetAuth("", "", "", time.Time{})
	if err != nil {
		log.Fatalln("Failed to recieve auth")
	}

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

	listener := startWorker(&listener.SQSAddr{
		Region:    aws.USWest2,
		QueueName: "spade-compactor-" + env,
		Auth:      auth,
	}, stats, postgresBackend)

	wait := make(chan struct{})

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT)
	go func() {
		<-sigc
		// Cause flush
		listener.Close()
		close(wait)
	}()

	<-wait
}

func startWorker(addr *listener.SQSAddr, stats statsd.Statter, b metadata.Storer) *listener.SQSListener {
	ret := listener.BuildSQSListener(addr, &rdsPipeHandler{
		MetadataStorer: b,
		Signer:         scoop_protocol.GetScoopSigner(),
		Statter:        stats,
	}, sqsPollWait)
	go ret.Listen()
	return ret
}

func (i *rdsPipeHandler) Handle(msg *sqs.Message) error {
	log.Printf("Got %s;%s\n", msg.Body, msg.MessageId)

	req, err := i.Signer.GetRowCopyRequest(strings.NewReader(msg.Body))
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
