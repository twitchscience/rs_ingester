package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/twitchscience/aws_utils/environment"
	"github.com/twitchscience/aws_utils/listener"

	"github.com/twitchscience/rs_ingester/keyring"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"

	"github.com/cactus/go-statsd-client/statsd"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/sqs"
)

var (
	poolSize               int
	loggingDir             string
	statsPrefix            string
	scoopHost              string
	scoopScheme            string
	keyRing                = keyring.New()
	alreadyCheckedOutError = errors.New("TableName is checked out")
	env                    = environment.GetCloudEnv()
)

type Stats interface {
	Timing(stat string, delta int64, rate float32) error
	Close() error
}

type IngestHandler struct {
	Auth    scoop_protocol.ScoopSigner
	Statter Stats
}

func (i *IngestHandler) Handle(msg *sqs.Message) error {
	start := time.Now()

	log.Printf("got %s;%s\n", msg.Body, msg.MessageId)

	req, err := i.Auth.GetRowCopyRequest(strings.NewReader(msg.Body))
	if err != nil {
		return err
	}

	if !keyRing.Checkout(req.TableName) {
		return alreadyCheckedOutError
	}
	defer keyRing.Return(req.TableName)

	log.Printf("Sending RowCopy to scoop for %s\n", msg.MessageId)

	resp, err := http.Post(copyURL(), "application/json", strings.NewReader(msg.Body))

	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		log.Printf("Post failed with status code: %s", resp.Status)
		return errors.New(fmt.Sprintf("Post failed with status code: %s", resp.Status))
	}

	log.Printf("Got res for %s as %v\n", msg.MessageId, err)

	i.Statter.Timing(req.TableName, int64(time.Now().Sub(start)), 1)
	return nil
}

func StartWorkers(addr *listener.SQSAddr, stats Stats) ([]*listener.SQSListener, error) {
	listeners := make([]*listener.SQSListener, poolSize)
	for i := 0; i < poolSize; i++ {
		listeners[i] = listener.BuildSQSListener(addr, &IngestHandler{
			Auth:    scoop_protocol.GetScoopSigner(),
			Statter: stats,
		}, 30*time.Second)
		go listeners[i].Listen()
	}
	return listeners, nil
}

func CloseListeners(listeners []*listener.SQSListener) {
	for _, l := range listeners {
		l.Close()
	}
}

func InitLogger(logDir string) (*os.File, error) {
	file, err := os.Create(
		fmt.Sprintf("%v/%v.%v.log", logDir, "Ingester", time.Now().Format("2006-01-02.15:04:00")))
	if err != nil {
		return nil, err
	}
	log.SetOutput(file)
	return file, nil
}

// copyURL builds and returns the scoop row copy URL from flags
func copyURL() string {
	u := url.URL{
		Scheme: scoopScheme,
		Host:   scoopHost,
		Path:   "/rows/copy",
	}
	return u.String()
}

func init() {
	flag.IntVar(&poolSize, "n_workers", 5, "how many connections to redshift should we form?")
	flag.StringVar(&loggingDir, "logging", "", "where are we logging")
	flag.StringVar(&statsPrefix, "statsPrefix", "ingester", "the prefix to statsd")
	flag.StringVar(&scoopHost, "scoopHost", "localhost:8080", "Scoop server host or host:port")
	flag.StringVar(&scoopScheme, "scoopScheme", "http", "Scoop server URL scheme (e.g. https)")
}

func main() {
	flag.Parse()

	_, err := InitLogger(loggingDir)
	if err != nil {
		log.Fatalln("Failed to start logger")
	}

	auth, err := aws.GetAuth("", "", "", time.Time{})
	if err != nil {
		log.Fatalln("Failed to recieve auth")
	}

	// Set up statsd monitoring
	// - If the env is not set up we wil use a noop connection
	statsdHostport := os.Getenv("STATSD_HOSTPORT")
	var stats Stats
	if statsdHostport == "" {
		// Error is meaningless here.
		stats, _ = statsd.NewNoop(statsdHostport, statsPrefix)
	} else {
		if stats, err = statsd.New(statsdHostport, statsPrefix); err != nil {
			log.Fatalf("Statsd configuration error: %v", err)
		}
		log.Printf("Connected to statsd at %s\n", statsdHostport)
	}

	listeners, err := StartWorkers(
		&listener.SQSAddr{
			Region:    aws.USWest2,
			QueueName: "spade-compactor-" + env,
			Auth:      auth,
		},
		stats,
	)

	if err != nil {
		log.Fatal(err)
	}

	wait := make(chan bool)

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc,
		syscall.SIGINT)
	go func() {
		<-sigc
		// Cause flush
		CloseListeners(listeners)
		stats.Close()
		wait <- true
	}()

	<-wait
}
