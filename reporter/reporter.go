package reporter

import (
	"fmt"
	"time"

	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/rs_ingester/metadata"
	"github.com/twitchscience/rs_ingester/monitoring"
)

// Reporter that queries a backend in intervals and sends stats.
type Reporter struct {
	backend    metadata.Reader
	stats      monitoring.SafeStatter
	pollPeriod time.Duration
	closer     chan bool
}

// New returns a Reporter that polls from backend with a given interval.
func New(backend metadata.Reader, stats monitoring.SafeStatter, pollPeriod time.Duration) *Reporter {
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

func (r *Reporter) sendInQueueEventStats() error {
	events, err := r.backend.EventsInQueue()
	if err != nil {
		return err
	}

	logger.WithField("count", len(events)).Infof("Found events in queue for loading")
	var totalInQueueCount int64
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
		r.stats.SafeGauge(fmt.Sprintf("tsv_files.%s.in_queue_count", event.Name), event.Count, 1.0)
		r.stats.SafeGauge(fmt.Sprintf("tsv_files.%s.age_in_ms", event.Name), ageInMS, 1.0)
		totalInQueueCount += event.Count
		if ageInMS > maxAgeInMS {
			maxAgeInMS = ageInMS
		}
	}
	r.stats.SafeGauge("tsv_files.total_in_queue_count", totalInQueueCount, 1.0)
	r.stats.SafeGauge("tsv_files.max_age_in_ms", maxAgeInMS, 1.0)
	return nil
}

func (r *Reporter) sendStaleEventStats() error {
	events, err := r.backend.StaleEvents()
	if err != nil {
		return err
	}

	logger.WithField("count", len(events)).Infof("Found stale events")
	var totalStaleCount int64
	for _, event := range events {
		r.stats.SafeGauge(fmt.Sprintf("tsv_files.%s.stale_count", event.Name), event.Count, 1.0)
		totalStaleCount += event.Count
	}
	r.stats.SafeGauge("tsv_files.total_stale_count", totalStaleCount, 1.0)
	return nil
}

func (r *Reporter) sendStats() error {
	err := r.sendInQueueEventStats()
	if err != nil {
		return err
	}
	return r.sendStaleEventStats()
}

// Close is a blocking function that waits to cleanly shut down reporting.
func (r *Reporter) Close() {
	r.closer <- true
}
