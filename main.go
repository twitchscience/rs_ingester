package main

import (
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/twitchscience/rs_ingester/keyring"

	"github.com/twitchscience/rs_ingester/healthcheck"
	"github.com/twitchscience/rs_ingester/lib"
	"github.com/twitchscience/rs_ingester/loadclient"
	"github.com/twitchscience/rs_ingester/metadata"
)

var (
	poolSize               int
	statsPrefix            string
	scoopURL               string
	manifestBucketPrefix   string
	keyRing                = keyring.New()
	alreadyCheckedOutError = errors.New("TableName is checked out")
	pgConfig               metadata.PGConfig
	loadAgeSeconds         int
	workerGroup            sync.WaitGroup
)

type LoadWorker struct {
	MetadataBackend metadata.MetadataBackend
	Loader          loadclient.Loader
}

func (i *LoadWorker) Work() error {

	c := i.MetadataBackend.LoadReady()
	for load := range c {
		log.Printf("Loading manifest %s (%d files) into table %s", load.UUID, len(load.Loads), load.TableName)
		err := i.Loader.LoadManifest(load)
		if err != nil {
			if err.Retryable() {
				i.MetadataBackend.LoadError(load.UUID, err.Error())
			}
			log.Printf("Error loading: %s, retryable: %t", err.Error(), err.Retryable())
			continue
		}
		log.Printf("Loaded manifest %s", load.UUID)
		i.MetadataBackend.LoadDone(load.UUID)
	}
	workerGroup.Done()
	return nil
}

func StartWorkers(b metadata.MetadataBackend, stats lib.Stats) ([]LoadWorker, error) {
	workers := make([]LoadWorker, poolSize)
	for i := 0; i < poolSize; i++ {
		loadclient, err := loadclient.NewScoopLoader(scoopURL, manifestBucketPrefix, stats)
		if err != nil {
			return workers, err
		}
		workers[i] = LoadWorker{MetadataBackend: b, Loader: loadclient}
		go workers[i].Work()
		workerGroup.Add(1)
	}
	return workers, nil
}

func init() {
	flag.StringVar(&statsPrefix, "statsPrefix", "ingester", "the prefix to statsd")
	flag.StringVar(&scoopURL, "scoopURL", "", "scoop url, like https://scoop.example.com")
	flag.StringVar(&pgConfig.DatabaseURL, "databaseURL", "", "Postgres-scheme url for the RDS instance")
	flag.StringVar(&manifestBucketPrefix, "manifestBucketPrefix", "", "Prefix for the S3 bucket for manifests. '-$CLOUD_ENVIRONMENT' will be appended for the actual bucket name")
	flag.IntVar(&pgConfig.LoadCountTrigger, "loadCountTrigger", 5, "Number of queued loads before a load triggers")
	flag.IntVar(&pgConfig.MaxConnections, "maxDBConnections", 5, "Number of database connections to open")
	flag.StringVar(&pgConfig.TableWhitelist, "tableWhitelist", "", "If present, limits loads only to a comma-seperated list of tables")
	flag.IntVar(&loadAgeSeconds, "loadAgeSeconds", 1800, "Max age of queued load before it triggers")
	flag.IntVar(&poolSize, "n_workers", 5, "Number of load workers and therefore scoop connections")
}

func main() {
	flag.Parse()
	pgConfig.LoadAgeTrigger = time.Second * time.Duration(loadAgeSeconds)

	log.SetOutput(os.Stdout)
	stats, err := lib.InitStats(statsPrefix)
	if err != nil {
		log.Fatalln("Failed to setup statter", err)
	}
	scoopConnection, err := loadclient.NewScoopLoader(scoopURL, manifestBucketPrefix, stats)
	if err != nil {
		log.Fatalln("Failed to setup scoop client for postgres", err)
	}

	pgBackend, err := metadata.NewPostgresLoader(&pgConfig, scoopConnection)
	if err != nil {
		log.Fatalln("Failed to setup postgres backend", err)
	}

	_, err = StartWorkers(pgBackend, stats)
	if err != nil {
		log.Fatalln("Failed to start workers", err)
	}

	hcBackend := healthcheck.BuildHealthCheckBackend(scoopConnection, pgBackend)
	hcHandler := healthcheck.BuildHealthCheckHandler(hcBackend)

	hcServeMux := http.NewServeMux()
	hcServeMux.Handle("/health", healthcheck.MakeHealthRouter(hcHandler))

	go func() {
		if err := http.ListenAndServe(net.JoinHostPort("localhost", "8080"), hcServeMux); err != nil {
			log.Fatal("Health Check (HTTP) failed: ", err)
		}
	}()

	go func() {
		log.Println(http.ListenAndServe(":6060", nil))
	}()

	wait := make(chan struct{})
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT)
	log.Println("Loader is set up")
	go func() {
		<-sigc
		log.Println("Sigint received -- shutting down")
		pgBackend.Close()
		// Cause flush
		stats.Close()
		workerGroup.Wait()
		close(wait)
	}()
	<-wait
}
