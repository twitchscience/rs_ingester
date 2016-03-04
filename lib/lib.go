package lib

import (
	"log"
	"os"

	"github.com/cactus/go-statsd-client/statsd"
)

// InitStats sets up the statsd monitoring, with a noop connection if
// STATSD_HOSTPORT is not set
func InitStats(statsPrefix string) (stats statsd.Statter, err error) {
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
