package lib

import (
	"log"
	"os"

	"github.com/cactus/go-statsd-client/statsd"
)

func InitStats(statsPrefix string) (stats statsd.Statter, err error) {
	// Set up statsd monitoring
	// - If the env is not set up we wil use a noop connection
	statsdHostport := os.Getenv("STATSD_HOSTPORT")

	if statsdHostport == "" {
		// Error is meaningless here.
		stats, _ = statsd.NewNoop(statsdHostport, statsPrefix)
		log.Println("Running statsd with noop client.")
	} else {
		if stats, err = statsd.New(statsdHostport, statsPrefix); err != nil {
			log.Printf("Error connecting to statsd server at %s\n", statsdHostport)
			return
		}
		log.Printf("Connected to statsd at %s\n", statsdHostport)
	}
	return
}
