package monitoring

import (
	"os"

	"github.com/cactus/go-statsd-client/statsd"
	"github.com/twitchscience/aws_utils/logger"
)

// SafeStatter is a statter that sends stats and doesn't return errors.
type SafeStatter interface {
	statsd.Statter
	SafeInc(stat string, value int64, rate float32)
	SafeGauge(stat string, value int64, rate float32)
	SafeTiming(stat string, value int64, rate float32)
}

// LoggingStatter is a statter that logs errors.
type LoggingStatter struct {
	statsd.Statter
}

// SafeInc increments a stat, logging errors.
func (s *LoggingStatter) SafeInc(stat string, value int64, rate float32) {
	err := s.Inc(stat, value, rate)
	if err != nil {
		logger.WithError(err).Errorf("Error sending stat %s Inc to statsd", stat)
	}
}

// SafeGauge submits/updates a gauge, logging errors.
func (s *LoggingStatter) SafeGauge(stat string, value int64, rate float32) {
	err := s.Gauge(stat, value, rate)
	if err != nil {
		logger.WithError(err).Errorf("Error sending stat %s Gauge to statsd", stat)
	}
}

// SafeTiming submits a timing, logging errors.
func (s *LoggingStatter) SafeTiming(stat string, value int64, rate float32) {
	err := s.Timing(stat, value, rate)
	if err != nil {
		logger.WithError(err).Errorf("Error sending stat %s Timing to statsd", stat)
	}
}

// InitStats sets up the statsd monitoring, with a noop connection if
// STATSD_HOSTPORT is not set
func InitStats(statsPrefix string) (*LoggingStatter, error) {
	statsdHostport := os.Getenv("STATSD_HOSTPORT")
	var stats statsd.Statter
	var err error

	if statsdHostport == "" {
		// Error is meaningless here.
		stats, _ = statsd.NewNoop(statsdHostport, statsPrefix)
		logger.Info("Running statsd with noop client")
	} else {
		if stats, err = statsd.New(statsdHostport, statsPrefix); err != nil {
			logger.WithError(err).Errorf("Error connecting to statsd server at %s", statsdHostport)
			return nil, err
		}
		logger.Infof("Connected to statsd at %s", statsdHostport)

	}
	return &LoggingStatter{Statter: stats}, nil
}
