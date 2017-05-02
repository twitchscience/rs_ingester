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

func (r *Reporter) sendStats() error {
	allStats, err := r.backend.StatsForPendingLoads()
	if err != nil {
		return err
	}

	pendingLoadsCnt := 0
	for _, pendingLoadStats := range allStats {
		pendingLoadsCnt += len(pendingLoadStats.Stats)
		r.sendPendingLoadStats(pendingLoadStats)
	}
	if pendingLoadsCnt > 0 {
		logger.WithField("count", pendingLoadsCnt).Info("Found events in queue for loading")
	} else {
		logger.Info("Found no events in queue for loading")
	}
	return nil
}

// Close is a blocking function that waits to cleanly shut down reporting.
func (r *Reporter) Close() {
	r.closer <- true
}

// sendPendingLoadStats sends stats for events pending load to graphite under a particular label.
func (r *Reporter) sendPendingLoadStats(pendingLoadStats *metadata.PendingLoadStats) {
	var totalCount int64
	var maxAgeInMS int64
	label := pendingLoadStats.Type
	for _, eventStats := range pendingLoadStats.Stats {
		var ageInMS int64
		metricPrefix := fmt.Sprintf("tsv_files.%s.%s", eventStats.Event, label)
		r.stats.SafeGauge(metricPrefix+"_count", eventStats.Count, 1.0)
		r.stats.SafeGauge(metricPrefix+"_age_in_ms", ageInMS, 1.0)
		totalCount += eventStats.Count
		if ageInMS > maxAgeInMS {
			maxAgeInMS = ageInMS
		}
	}
	r.stats.SafeGauge(fmt.Sprintf("tsv_files.%s_total_count", label), totalCount, 1.0)
	r.stats.SafeGauge(fmt.Sprintf("tsv_files.%s_max_age_in_ms", label), maxAgeInMS, 1.0)
}
