package main

import (
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

	"github.com/cactus/go-statsd-client/statsd"
	"github.com/twitchscience/rs_ingester/control"

	"github.com/twitchscience/rs_ingester/backend"
	"github.com/twitchscience/rs_ingester/healthcheck"
	"github.com/twitchscience/rs_ingester/lib"
	"github.com/twitchscience/rs_ingester/loadclient"
	"github.com/twitchscience/rs_ingester/metadata"
)

const (
	healthCheckPoolSize = 1
)

var (
	poolSize             int
	statsPrefix          string
	manifestBucketPrefix string
	rsURL                string
	pgConfig             metadata.PGConfig
	loadAgeSeconds       int
	workerGroup          sync.WaitGroup
)

type loadWorker struct {
	MetadataBackend metadata.Backend
	Loader          loadclient.Loader
}

func (i *loadWorker) Work() {

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
		log.Printf("Loaded manifest %s into table %s", load.UUID, load.TableName)
		i.MetadataBackend.LoadDone(load.UUID)
	}
	workerGroup.Done()
}

func startWorkers(b metadata.Backend, stats statsd.Statter, rsBackend *backend.RedshiftBackend) ([]loadWorker, error) {
	workers := make([]loadWorker, poolSize)
	for i := 0; i < poolSize; i++ {
		loadclient, err := loadclient.NewRSLoader(rsBackend, manifestBucketPrefix, stats)
		if err != nil {
			return workers, err
		}
		workers[i] = loadWorker{MetadataBackend: b, Loader: loadclient}
		go workers[i].Work()
		workerGroup.Add(1)
	}
	return workers, nil
}

func init() {
	flag.StringVar(&statsPrefix, "statsPrefix", "ingester", "the prefix to statsd")
	flag.StringVar(&pgConfig.DatabaseURL, "databaseURL", "", "Postgres-scheme url for the RDS instance")
	flag.StringVar(&manifestBucketPrefix, "manifestBucketPrefix", "", "Prefix for the S3 bucket for manifests. '-$CLOUD_ENVIRONMENT' will be appended for the actual bucket name")
	flag.IntVar(&pgConfig.LoadCountTrigger, "loadCountTrigger", 5, "Number of queued loads before a load triggers")
	flag.IntVar(&pgConfig.MaxConnections, "maxDBConnections", 5, "Number of database connections to open")
	flag.StringVar(&pgConfig.TableWhitelist, "tableWhitelist", "", "If present, limits loads only to a comma-seperated list of tables")
	flag.IntVar(&loadAgeSeconds, "loadAgeSeconds", 1800, "Max age of queued load before it triggers")
	flag.IntVar(&poolSize, "n_workers", 5, "Number of load workers and therefore redshift connections")
	flag.StringVar(&rsURL, "rsURL", "", "URL for Redshift")
}

func main() {
	flag.Parse()
	pgConfig.LoadAgeTrigger = time.Second * time.Duration(loadAgeSeconds)

	log.SetOutput(os.Stdout)
	stats, err := lib.InitStats(statsPrefix)
	if err != nil {
		log.Fatalln("Failed to setup statter", err)
	}

	rsBackend, err := backend.BuildRedshiftBackend(poolSize+healthCheckPoolSize, rsURL)

	if err != nil {
		log.Fatalln("Failed to setup redshift connection", err)
	}

	rsConnection, err := loadclient.NewRSLoader(rsBackend, manifestBucketPrefix, stats)
	if err != nil {
		log.Fatalln("Failed to setup Redshift loading client for postgres", err)
	}

	pgBackend, err := metadata.NewPostgresLoader(&pgConfig, rsConnection)
	if err != nil {
		log.Fatalln("Failed to setup postgres backend", err)
	}

	_, err = startWorkers(pgBackend, stats, rsBackend)
	if err != nil {
		log.Fatalln("Failed to start workers", err)
	}

	hcBackend := healthcheck.NewBackend(rsConnection, pgBackend)
	hcHandler := healthcheck.NewHandler(hcBackend)

	serveMux := http.NewServeMux()
	serveMux.Handle("/health", healthcheck.NewHealthRouter(hcHandler))

	db, err := metadata.ConnectToDB(pgConfig.DatabaseURL, pgConfig.MaxConnections)
	if err != nil {
		log.Fatalln("Failed to set up postgres connection", err)
	}
	controlBackend := control.NewControlBackend(db)
	controlHandler := control.NewControlHandler(controlBackend, stats)

	serveMux.Handle("/control/ingest", control.NewControlRouter(controlHandler))
	serveMux.Handle("/loads/tables", control.NewControlRouter(controlHandler))

	go func() {
		if err = http.ListenAndServe(net.JoinHostPort("localhost", "8080"), serveMux); err != nil {
			log.Fatal("Serving health and control failed: ", err)
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
		err = stats.Close()
		if err != nil {
			log.Printf("Error closing statter: %s", err)
		}
		workerGroup.Wait()
		close(wait)
	}()
	<-wait
}
