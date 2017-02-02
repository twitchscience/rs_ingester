package reporter

import (
	"reflect"
	"testing"
	"time"

	"github.com/cactus/go-statsd-client/statsd"
	"github.com/cactus/go-statsd-client/statsd/statsdtest"

	"github.com/twitchscience/rs_ingester/metadata"
)

// MockReader mocks what's minimally required to obtain a custom list of events pending loads
type MockReader struct {
	PendingLoad []metadata.Event
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
func (m *MockReader) PrioritizeTSVVersion(table string, version int) error {
	return nil
}
func (m *MockReader) EventsPendingLoad() ([]metadata.Event, error) {
	return m.PendingLoad, nil
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
		[]metadata.Event{
			{
				Name:  "event_1",
				Count: 1,
				MinTS: unixEpoch,
			},
			{
				Name:  "event_2",
				Count: 2,
				MinTS: unixEpoch,
			},
		},
	}

	r := &Reporter{
		backend: mockBackend,
		stats:   statter,
	}
	err = r.sendStats()
	if err != nil {
		t.Fatalf("failed to send stats: %s", err)
	}

	statsSent := rs.GetSent()
	if len(statsSent) != 6 {
		t.Fatalf("failed to capture right amount of events; got: %d, expected: 6", len(statsSent))
	}
	expectedStats := statsdtest.Stats{
		{[]byte("t.tsv_files.event_1.count:1|s"), "t.tsv_files.event_1.count", "1", "s", "", true},
		{[]byte("t.tsv_files.event_1.age_in_ms:0|s"), "t.tsv_files.event_1.age_in_ms", "0", "s", "", true},
		{[]byte("t.tsv_files.event_2.count:2|s"), "t.tsv_files.event_2.count", "2", "s", "", true},
		{[]byte("t.tsv_files.event_2.age_in_ms:0|s"), "t.tsv_files.event_2.age_in_ms", "0", "s", "", true},
		{[]byte("t.tsv_files.total_count:3|s"), "t.tsv_files.total_count", "3", "s", "", true},
		{[]byte("t.tsv_files.max_age_in_ms:0|s"), "t.tsv_files.max_age_in_ms", "0", "s", "", true},
	}
	if !reflect.DeepEqual(statsSent, expectedStats) {
		t.Fatalf("Failed to send right stats:\ngot: %s,\nwant: %s", statsSent, expectedStats)
	}
}
