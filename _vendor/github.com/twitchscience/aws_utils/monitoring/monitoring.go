package monitoring

import (
	"time"

	"github.com/cactus/go-statsd-client/statsd"
	"github.com/twitchscience/aws_utils/logger"
)

// SafeStatter is a statter that sends stats and doesn't return errors.
type SafeStatter interface {
	SafeInc(stat string, value int64, rate float32)
	SafeGauge(stat string, value int64, rate float32)
	SafeTimingDuration(stat string, delta time.Duration, rate float32)
}

// LoggingStatter is a statter that logs errors.
type LoggingStatter struct {
	statsd.Statter
}

// SafeInc increments a stat, logging errors.
func (s *LoggingStatter) SafeInc(stat string, value int64, rate float32) {
	if err := s.Inc(stat, value, rate); err != nil {
		logger.WithError(err).Errorf("Error sending stat %s Inc to statsd", stat)
	}
}

// SafeGauge submits/updates a gauge, logging errors.
func (s *LoggingStatter) SafeGauge(stat string, value int64, rate float32) {
	if err := s.Gauge(stat, value, rate); err != nil {
		logger.WithError(err).Errorf("Error sending stat %s Gauge to statsd", stat)
	}
}

// SafeTimingDuration submits a statsd timing type, logging errors
func (s *LoggingStatter) SafeTimingDuration(stat string, delta time.Duration, rate float32) {
	if err := s.TimingDuration(stat, delta, rate); err != nil {
		logger.WithError(err).Errorf("Error sending stat %s TimingDuration to statsd", stat)
	}
}

// NewStatter sets up the statsd monitoring
func NewStatter(statsdURL, statsPrefix string) (*LoggingStatter, error) {
	stats, err := statsd.NewClient(statsdURL, statsPrefix)
	if err != nil {
		return nil, err
	}
	return &LoggingStatter{Statter: stats}, nil
}

type mockStatter struct{}

// NewMockStatter returns a mock statter
func NewMockStatter() SafeStatter {
	return &mockStatter{}
}

func (m *mockStatter) SafeInc(stat string, value int64, rate float32)                    {}
func (m *mockStatter) SafeGauge(stat string, value int64, rate float32)                  {}
func (m *mockStatter) SafeTimingDuration(stat string, delta time.Duration, rate float32) {}
