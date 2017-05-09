package reporter

import (
	"testing"
	"time"

	"github.com/cactus/go-statsd-client/statsd"
	"github.com/cactus/go-statsd-client/statsd/statsdtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

type mockClock struct{}

func (mockClock) Since(t time.Time) time.Duration {
	return time.Date(2017, 1, 2, 0, 0, 0, 0, time.UTC).Sub(t)
}

// TestSendStats check we send the right stats given a fixed list of pending events
func TestSendStats(t *testing.T) {
	rs := new(statsdtest.RecordingSender)
	statter, err := statsd.NewClientWithSender(rs, "t")
	if err != nil {
		t.Fatal("failed to construct statter for testing")
	}

	someTime, err := time.Parse("Jan 2, 2006 UTC", "Jan 1, 2017 UTC")
	if err != nil {
		t.Fatal("failed to parse example time")
	}

	mockBackend := &MockReader{
		[]*metadata.PendingLoadStats{
			{
				Type: metadata.PendingInQueue,
				Stats: []*metadata.EventStats{
					{Event: "event_1", Count: 1, MinTS: someTime},
					{Event: "event_2"},
				},
			},
			{
				Type: metadata.PendingStale,
				Stats: []*metadata.EventStats{
					{Event: "event_1", MinTS: time.Time{}},
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
		clock:   mockClock{},
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
		// 86400000 is the difference between Jan. 2 2017 (the day the clock pretends it is) and Jan. 1 2017 (our MinTS)
		{[]byte("t.tsv_files.event_1.in_queue_age_in_ms:86400000|g"), "t.tsv_files.event_1.in_queue_age_in_ms", "86400000", "g", "", true},
		{[]byte("t.tsv_files.event_2.in_queue_count:0|g"), "t.tsv_files.event_2.in_queue_count", "0", "g", "", true},
		{[]byte("t.tsv_files.event_2.in_queue_age_in_ms:0|g"), "t.tsv_files.event_2.in_queue_age_in_ms", "0", "g", "", true},
		{[]byte("t.tsv_files.in_queue_total_count:1|g"), "t.tsv_files.in_queue_total_count", "1", "g", "", true},
		{[]byte("t.tsv_files.in_queue_max_age_in_ms:86400000|g"), "t.tsv_files.in_queue_max_age_in_ms", "86400000", "g", "", true},

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
	require.Equal(t, len(expectedStats), len(statsSent))
	for i, expected := range expectedStats {
		if assert.Equal(t, string(expected.Raw), string(statsSent[i].Raw)) {
			assert.Equal(t, expected, statsSent[i])
		}
	}
}
