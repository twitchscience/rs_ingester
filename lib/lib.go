package lib

import (
	"os"

	"github.com/cactus/go-statsd-client/statsd"
	"github.com/twitchscience/aws_utils/logger"
)

// InitStats sets up the statsd monitoring, with a noop connection if
// STATSD_HOSTPORT is not set
func InitStats(statsPrefix string) (stats statsd.Statter, err error) {
	statsdHostport := os.Getenv("STATSD_HOSTPORT")

	if statsdHostport == "" {
		// Error is meaningless here.
		stats, _ = statsd.NewNoop(statsdHostport, statsPrefix)
		logger.Info("Running statsd with noop client")
	} else {
		if stats, err = statsd.New(statsdHostport, statsPrefix); err != nil {
			logger.WithError(err).Errorf("Error connecting to statsd server at %s", statsdHostport)
			return
		}
		logger.Infof("Connected to statsd at %s", statsdHostport)
	}
	return
}
