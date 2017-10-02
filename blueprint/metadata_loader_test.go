package blueprint

import (
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/aws_utils/monitoring"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

var (
	knownEventMetadataOne = scoop_protocol.EventMetadataConfig{
		Metadata: map[string](map[string]scoop_protocol.EventMetadataRow){
			"test-event-one": map[string]scoop_protocol.EventMetadataRow{
				"edge_type": scoop_protocol.EventMetadataRow{
					MetadataValue: "internal",
					UserName:      "unknown",
					Version:       1,
				},
				"comment": scoop_protocol.EventMetadataRow{
					MetadataValue: "test comment",
					UserName:      "unknown",
					Version:       1,
				},
			},
		},
	}
	knownEventMetadataTwo = scoop_protocol.EventMetadataConfig{
		Metadata: make(map[string](map[string]scoop_protocol.EventMetadataRow)),
	}
)

func TestRefresh(t *testing.T) {
	loader, err := NewMetadataLoader(
		&mockFetcher{
			failFetch: []bool{false, false},
			configs: []scoop_protocol.EventMetadataConfig{
				knownEventMetadataOne,
				knownEventMetadataTwo,
			},
		},
		1*time.Microsecond,
		1,
		monitoring.NewMockStatter(),
	)
	if err != nil {
		t.Fatalf("was expecting no error but got %v\n", err)
		t.FailNow()
	}

	metadata := loader.GetAllMetadata().Metadata["Metadata"]
	if len(metadata) == 0 {
		t.Fatal("expected metadata to be non-empty")
		t.FailNow()
	}

	go loader.Crank()
	time.Sleep(101 * time.Millisecond)

	metadata = loader.GetAllMetadata().Metadata["Metadata"]
	logger.Error(metadata)
	if len(metadata) != 0 {
		t.Fatal("expected metadata to be empty")
		t.FailNow()
	}
	loader.Close()
}

func TestRetryPull(t *testing.T) {
	_, err := NewMetadataLoader(
		&mockFetcher{
			failFetch: []bool{true, true, true, true, true},
		},
		1*time.Second,
		1*time.Microsecond,
		monitoring.NewMockStatter(),
	)
	if err == nil {
		t.Fatalf("expected loader to timeout\n")
		t.FailNow()
	}
}

type mockFetcher struct {
	failFetch []bool
	configs   []scoop_protocol.EventMetadataConfig

	i int
}

type testReadWriteCloser struct {
	config scoop_protocol.EventMetadataConfig
}

func (trwc *testReadWriteCloser) Read(p []byte) (int, error) {
	var b []byte
	b, _ = json.Marshal(trwc.config)
	return copy(p, b), io.EOF
}

func (trwc *testReadWriteCloser) Write(p []byte) (int, error) {
	return len(p), nil
}

func (trwc *testReadWriteCloser) Close() error {
	return nil
}

func (t *mockFetcher) FetchAndWrite(r io.ReadCloser, w io.WriteCloser) error {
	return nil
}

func (t *mockFetcher) Fetch() (io.ReadCloser, error) {
	if len(t.failFetch) < t.i && t.failFetch[t.i] {
		t.i++
		return nil, fmt.Errorf("failed on %d try", t.i)
	}
	if len(t.configs)-1 < t.i {
		return nil, fmt.Errorf("failed on %d try", t.i)
	}
	rc := &testReadWriteCloser{
		config: t.configs[t.i],
	}
	t.i++
	return rc, nil
}

func (t *mockFetcher) ConfigDestination(d string) (io.WriteCloser, error) {
	return &testReadWriteCloser{
		config: knownEventMetadataOne,
	}, nil
}
