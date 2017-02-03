package reporter

import (
	"fmt"
	"time"

	"github.com/cactus/go-statsd-client/statsd"

	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/rs_ingester/metadata"
)

// Reporter that queries a backend in intervals and sends stats.
type Reporter struct {
	backend    metadata.Reader
	stats      statsd.Statter
	pollPeriod time.Duration
	closer     chan bool
}

// New returns a Reporter that polls from backend with a given interval.
func New(backend metadata.Reader, stats statsd.Statter, pollPeriod time.Duration) *Reporter {
	r := &Reporter{
		backend:    backend,
		stats:      stats,
		pollPeriod: pollPeriod,
		closer:     make(chan bool),
	}
	logger.Go(r.reporterThread)
	return r
}

func (r *Reporter) reporterThread() {
	logger.Info("Reporter started.")
	defer logger.Info("Reporter stopped.")
	tick := time.NewTicker(r.pollPeriod)
	for {
		select {
		case <-tick.C:
			err := r.sendStats()
			if err != nil {
				logger.WithError(err).Error("failed to report stats")
				continue
			}
		case <-r.closer:
			return
		}
	}
}

func (r *Reporter) sendStats() error {
	events, err := r.backend.EventsPendingLoad()
	if err != nil {
		return err
	}
	logger.Infof("Found %d events pending load", len(events))
	var totalCount int64
	var maxAgeInMS int64
	for _, event := range events {
		var ageInMS int64
		if event.MinTS.Unix() == 0 {
			// To force-load an event, we change the ts to the Unix epoch. For now we'll handle
			// this case as age 0 to not mess with the metrics
			ageInMS = 0
		} else {
			ageInMS = int64(time.Since(event.MinTS) / time.Millisecond)
		}
		_ = r.stats.Gauge(fmt.Sprintf("tsv_files.%s.count", event.Name), event.Count, 1.0)
		_ = r.stats.Gauge(fmt.Sprintf("tsv_files.%s.age_in_ms", event.Name), ageInMS, 1.0)
		totalCount += event.Count
		if ageInMS > maxAgeInMS {
			maxAgeInMS = ageInMS
		}
	}
	_ = r.stats.Gauge("tsv_files.total_count", totalCount, 1.0)
	_ = r.stats.Gauge("tsv_files.max_age_in_ms", maxAgeInMS, 1.0)
	return nil
}

// Close is a blocking function that waits to cleanly shut down reporting.
func (r *Reporter) Close() {
	r.closer <- true
}
