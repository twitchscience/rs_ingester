package lib

import (
	"log"
	"os"

	"github.com/cactus/go-statsd-client/statsd"
)

type Stats interface {
	Timing(stat string, delta int64, rate float32) error
	Close() error
}

func InitStats(statsPrefix string) (stats Stats, err error) {
	// Set up statsd monitoring
	// - If the env is not set up we wil use a noop connection
	statsdHostport := os.Getenv("STATSD_HOSTPORT")

	if statsdHostport == "" {
		// Error is meaningless here.
		stats, _ = statsd.NewNoop(statsdHostport, statsPrefix)
	} else {
		if stats, err = statsd.New(statsdHostport, statsPrefix); err != nil {
			return
		}
		log.Printf("Connected to statsd at %s\n", statsdHostport)
	}
	return
}
