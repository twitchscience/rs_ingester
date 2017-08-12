package blueprint

import (
	"encoding/json"
	"io/ioutil"
	"math/rand"
	"time"

	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/rs_ingester/monitoring"
	// "github.com/twitchscience/rs_ingester/fetcher"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

// StaticLoader is a static set of transformers and versions.
// type StaticLoader struct {
// 	configs scoop_protocol.EventMetadataConfig
// }

// NewStaticLoader creates a StaticLoader from the given event metadata config
// func NewStaticLoader(config scoop_protocol.EventMetadataConfig) *StaticLoader {
// 	return &StaticLoader{
// 		configs: config,
// 	}
// }

// MetadataLoader fetches configs on an interval, with stats on the fetching process.
type MetadataLoader struct {
	fetcher    ConfigFetcher
	reloadTime time.Duration
	retryDelay time.Duration
	configs    scoop_protocol.EventMetadataConfig

	closer chan bool
	stats  monitoring.SafeStatter
	// stats    reporter.StatsLogger
}

// NewDynamicLoader returns a new MetadataLoader, performing the first fetch.
// func NewMetadataLoader(
// 	fetcher fetcher.ConfigFetcher,
// 	reloadTime time.Duration,
// 	retryDelay time.Duration,
// 	// stats monitoring.SafeStatter,
// 	// stats reporter.StatsLogger,
// ) (*MetadataLoader, error) {
// 	d := MetadataLoader{
// 		fetcher:    fetcher,
// 		reloadTime: reloadTime,
// 		retryDelay: retryDelay,
// 		configs:    scoop_protocol.EventMetadataConfig{},
// 		closer:     make(chan bool),
// 		// stats:      stats,
// 	}

// 	config, err := d.retryPull(5, retryDelay)
// 	if err != nil {
// 		return nil, err
// 	}
// 	d.configs = config
// 	return &d, nil
// }

// NewMetadataLoader returns a new MetadataLoader, performing the first fetch.
func NewMetadataLoader(
	fetcher ConfigFetcher,
	reloadTime time.Duration,
	retryDelay time.Duration,
	stats monitoring.SafeStatter,
	// stats reporter.StatsLogger,
) (*MetadataLoader, error) {
	d := MetadataLoader{
		fetcher:    fetcher,
		reloadTime: reloadTime,
		retryDelay: retryDelay,
		configs:    scoop_protocol.EventMetadataConfig{},
		closer:     make(chan bool),
		stats:      stats,
	}

	config, err := d.retryPull(5, retryDelay)
	if err != nil {
		return nil, err
	}
	d.configs = config
	return &d, nil
}

// GetMetadataValueByType returns the metadata value given an eventName and metadataType
// func (s *StaticLoader) GetMetadataValueByType(eventName string, metadataType string) string {
// 	if eventMetadata, found := s.configs.Metadata[eventName]; found {
// 		if metadataRow, exists := eventMetadata[metadataType]; exists {
// 			return metadataRow.MetadataValue
// 		}
// 	}
// 	return ""
// }

// GetMetadataValueByType returns the metadata value given an eventName and metadataType
func (d *MetadataLoader) GetMetadataValueByType(eventName string, metadataType string) string {
	if eventMetadata, found := d.configs.Metadata[eventName]; found {
		if metadataRow, exists := eventMetadata[metadataType]; exists {
			return metadataRow.MetadataValue
		}
	}
	return ""
}

func (d *MetadataLoader) retryPull(n int, waitTime time.Duration) (scoop_protocol.EventMetadataConfig, error) {
	var err error
	var config scoop_protocol.EventMetadataConfig
	for i := 1; i <= n; i++ {
		config, err = d.pullConfigIn()
		if err == nil {
			return config, nil
		}
		time.Sleep(waitTime * time.Duration(i))
	}
	return config, err
}

func (d *MetadataLoader) pullConfigIn() (scoop_protocol.EventMetadataConfig, error) {
	configReader, err := d.fetcher.Fetch()
	if err != nil {
		return scoop_protocol.EventMetadataConfig{}, err
	}

	b, err := ioutil.ReadAll(configReader)
	if err != nil {
		return scoop_protocol.EventMetadataConfig{}, err
	}
	cfgs := scoop_protocol.EventMetadataConfig{
		Metadata: make(map[string](map[string]scoop_protocol.EventMetadataRow)),
	}
	err = json.Unmarshal(b, &cfgs.Metadata)
	if err != nil {
		return scoop_protocol.EventMetadataConfig{}, err
	}
	return cfgs, nil
}

// func (d *MetadataLoader) safeTiming()

// Close stops the MetadataLoader's fetching process.
func (d *MetadataLoader) Close() {
	d.closer <- true
}

// Crank is a blocking function that refreshes the config on an interval.
func (d *MetadataLoader) Crank() {
	// Jitter reload
	tick := time.NewTicker(d.reloadTime + time.Duration(rand.Intn(100))*time.Millisecond)
	for {
		select {
		case <-tick.C:
			// can put a circuit breaker here.
			now := time.Now()
			newConfig, err := d.retryPull(5, d.retryDelay)
			if err != nil {
				logger.WithError(err).Error("Failed to refresh config")
				d.stats.SafeTiming("config.error", int64(time.Since(now)), 0.1)
				continue
			}
			d.stats.SafeTiming("config.success", int64(time.Since(now)), 0.1)
			d.configs = newConfig
		case <-d.closer:
			tick.Stop()
			return
		}
	}
}
