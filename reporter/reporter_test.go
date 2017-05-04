package reporter

import (
	"reflect"
	"testing"
	"time"

	"github.com/cactus/go-statsd-client/statsd"
	"github.com/cactus/go-statsd-client/statsd/statsdtest"

	"github.com/twitchscience/rs_ingester/metadata"
	"github.com/twitchscience/rs_ingester/monitoring"
)

// MockReader mocks what's minimally required to obtain a custom list of events pending loads
type MockReader struct {
	pendingLoadsStats []*metadata.PendingLoadStats
}

func (m *MockReader) Versions() (map[string]int, error) {
	return nil, nil
}
func (m *MockReader) PingDB() error {
	return nil
}
func (m *MockReader) TSVVersionExists(table string, version int) (bool, error) {
	return false, nil
}
func (m *MockReader) ForceLoad(table string, requester string) error {
	return nil
}
func (m *MockReader) StatsForPendingLoads() ([]*metadata.PendingLoadStats, error) {
	return m.pendingLoadsStats, nil
}

// TestSendStats check we send the right stats given a fixed list of pending events
func TestSendStats(t *testing.T) {
	rs := new(statsdtest.RecordingSender)
	statter, err := statsd.NewClientWithSender(rs, "t")
	if err != nil {
		t.Fatal("failed to construct statter for testing")
	}

	unixEpoch, err := time.Parse("Jan 2, 2006 UTC", "Jan 1, 1970 UTC")
	if err != nil {
		t.Fatal("failed to parse unix epoch")
	}

	mockBackend := &MockReader{
		[]*metadata.PendingLoadStats{
			{
				Type: metadata.PendingInQueue,
				Stats: []*metadata.EventStats{
					{Event: "event_1", Count: 1},
					{Event: "event_2"},
				},
			},
			{
				Type: metadata.PendingStale,
				Stats: []*metadata.EventStats{
					{Event: "event_1", MinTS: unixEpoch},
					{Event: "event_2"},
				},
			},
			{
				Type: metadata.PendingMigration,
				Stats: []*metadata.EventStats{
					{Event: "event_1", Count: 2},
					{Event: "event_2", Count: 1},
				},
			},
		},
	}

	r := &Reporter{
		backend: mockBackend,
		stats:   &monitoring.LoggingStatter{Statter: statter},
	}
	err = r.sendStats()
	if err != nil {
		t.Fatalf("failed to send stats: %s", err)
	}

	statsSent := rs.GetSent()
	if len(statsSent) != 18 {
		t.Fatalf("failed to capture right amount of events; got: %d, expected: 18", len(statsSent))
	}
	expectedStats := statsdtest.Stats{
		// in queue
		{[]byte("t.tsv_files.event_1.in_queue_count:1|g"), "t.tsv_files.event_1.in_queue_count", "1", "g", "", true},
		{[]byte("t.tsv_files.event_1.in_queue_age_in_ms:0|g"), "t.tsv_files.event_1.in_queue_age_in_ms", "0", "g", "", true},
		{[]byte("t.tsv_files.event_2.in_queue_count:0|g"), "t.tsv_files.event_2.in_queue_count", "0", "g", "", true},
		{[]byte("t.tsv_files.event_2.in_queue_age_in_ms:0|g"), "t.tsv_files.event_2.in_queue_age_in_ms", "0", "g", "", true},
		{[]byte("t.tsv_files.in_queue_total_count:1|g"), "t.tsv_files.in_queue_total_count", "1", "g", "", true},
		{[]byte("t.tsv_files.in_queue_max_age_in_ms:0|g"), "t.tsv_files.in_queue_max_age_in_ms", "0", "g", "", true},

		// stale
		{[]byte("t.tsv_files.event_1.stale_count:0|g"), "t.tsv_files.event_1.stale_count", "0", "g", "", true},
		{[]byte("t.tsv_files.event_1.stale_age_in_ms:0|g"), "t.tsv_files.event_1.stale_age_in_ms", "0", "g", "", true},
		{[]byte("t.tsv_files.event_2.stale_count:0|g"), "t.tsv_files.event_2.stale_count", "0", "g", "", true},
		{[]byte("t.tsv_files.event_2.stale_age_in_ms:0|g"), "t.tsv_files.event_2.stale_age_in_ms", "0", "g", "", true},
		{[]byte("t.tsv_files.stale_total_count:0|g"), "t.tsv_files.stale_total_count", "0", "g", "", true},
		{[]byte("t.tsv_files.stale_max_age_in_ms:0|g"), "t.tsv_files.stale_max_age_in_ms", "0", "g", "", true},

		// pending migration
		{[]byte("t.tsv_files.event_1.pending_migration_count:2|g"), "t.tsv_files.event_1.pending_migration_count", "2", "g", "", true},
		{[]byte("t.tsv_files.event_1.pending_migration_age_in_ms:0|g"), "t.tsv_files.event_1.pending_migration_age_in_ms", "0", "g", "", true},
		{[]byte("t.tsv_files.event_2.pending_migration_count:1|g"), "t.tsv_files.event_2.pending_migration_count", "1", "g", "", true},
		{[]byte("t.tsv_files.event_2.pending_migration_age_in_ms:0|g"), "t.tsv_files.event_2.pending_migration_age_in_ms", "0", "g", "", true},
		{[]byte("t.tsv_files.pending_migration_total_count:3|g"), "t.tsv_files.pending_migration_total_count", "3", "g", "", true},
		{[]byte("t.tsv_files.pending_migration_max_age_in_ms:0|g"), "t.tsv_files.pending_migration_max_age_in_ms", "0", "g", "", true},
	}
	if !reflect.DeepEqual(statsSent, expectedStats) {
		t.Fatalf("Failed to send right stats:\ngot:\n%s\n,want:\n%s", statsSent, expectedStats)
	}
}
