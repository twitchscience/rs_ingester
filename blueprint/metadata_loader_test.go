// TODO: this whole file

// package blueprint

// import (
//  "encoding/json"
//  "fmt"
//  "io"
//  "testing"
//  "time"

//  "github.com/cactus/go-statsd-client/statsd"
//  "github.com/twitchscience/scoop_protocol/scoop_protocol"
//  "github.com/twitchscience/spade/geoip"
//  "github.com/twitchscience/spade/reporter"
// )

// import (
// 	"testing"
// 	"time"

// 	"github.com/aws/aws-sdk-go/service/s3"
// 	"github.com/aws/aws-sdk-go/service/s3/s3iface"
// 	"github.com/twitchscience/aws_utils/logger"
// 	"github.com/twitchscience/rs_ingester/monitoring"
// )

package blueprint

import (
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/twitchscience/rs_ingester/monitoring"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

var (
	// knownEventMetadataRowOne = (map[string]scoop_protocol.EventMetadataRow){
	// 	"edge_type": scoop_protocol.EventMetadataRow{
	// 		MetadataValue: "internal",
	// 		UserName:      "unknown",
	// 		Version:       1,
	// 	},
	// }
	knownEventMetadataOne = scoop_protocol.EventMetadataConfig{
		Metadata: map[string](map[string]scoop_protocol.EventMetadataRow){
			// 	"test-event-one": make(map[string]scoop_protocol.EventMetadataRow),
			// },
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
	// c, _ := statsd.NewNoop()
	stats, _ := monitoring.InitStats("scieng-test.ingester")
	loader, err := NewMetadataLoader(
		&mockFetcher{
			failFetch: []bool{
				false,
				false,
			},
			configs: []scoop_protocol.EventMetadataConfig{
				knownEventMetadataOne,
				knownEventMetadataTwo,
			},
		},
		1*time.Microsecond,
		1,
		stats,
	)
	if err != nil {
		t.Fatalf("was expecting no error but got %v\n", err)
		t.FailNow()

	}

	metadata := loader.GetAllMetadata()["Metadata"]
	// logger.Error(metadata"test-event-one"))
	// asdf, found := metadata["Metadata"]
	// if found {
	// 	logger.Error("Found")
	// 	logger.Error(asdf)
	// } else {
	// 	logger.Error("Not found")
	// 	logger.Error(len(metadata))
	// }
	// for k := range metadata {
	// 	// keys = append(keys, k)
	// 	logger.Error(k)
	// }
	// logger.Error(metadata)
	// logger.Error(metadata["test-event-one"])
	// 1
	// configs := loader.Print()
	// logger.Error(metadata)
	if len(metadata) == 0 {
		t.Fatal("expected metadata to be non-empty")
		t.FailNow()
	}
	// if err != nil {
	// 	logger.WithError(err).Error("ASdf")
	// }

	// value := loader.GetMetadataValueByType("test-event-one", "comment")
	// logger.Error(value)

	// configss, err := loader.GetMetadataValueByTypeWithError("test", metadataType)
	// configs, err = loader.GetMetadataValueByTypeWithError("test-event-two", "edge_type")
	// configs = loader.Print()
	// if err != nil {
	// 	logger.WithError(err).Error("ASdf")
	// }
	// logger.Error(configs)
	// value := loader.GetMetadataValueByType("", "comment")
	// if value != "" {
	// 	t.Fatal("expected empty string")
	// 	t.FailNow()
	// }
	// value = loader.GetMetadataValueByType("this-event-does-not-exist", "comment")
	// if value != "" {
	// 	t.Fatal("expected empty string")
	// 	t.FailNow()
	// }

	go loader.Crank()
	time.Sleep(101 * time.Millisecond)

	// configs = loader.Print()
	// if err != nil {
	// 	logger.WithError(err).Error("ASdf")
	// }
	// logger.Error(configs)

	metadata = loader.GetAllMetadata()["Metadata"]
	// logger.Error(metadata)
	// metadata = loader.GetAllMetadata()
	// logger.Error(len(metadata))
	// logger.Error(metadata)
	// logger.Error(metadata["test-event-one"])
	// 1
	// configs := loader.Print()
	if len(metadata) != 0 {
		t.Fatal("expected metadata to be empty")
		t.FailNow()
	}

	// value = loader.GetMetadataValueByType("test-event-one", "comment")
	// logger.Error(value)
	// if err != nil {
	// 	logger.WithError(err).Error("ASdf")
	// }
	// configs, err = loader.GetMetadataValueByTypeWithError("test-event-two", "edge_type")
	// if err != nil {
	// 	logger.WithError(err).Error("ASdf")
	// }
	// logger.Error(configs)
	// configs = loader.GetMetadataRow("test-event-one", "edge_type")
	// logger.Error(configs)/

	// value := loader.GetMetadataValueByType("test-event-two", "comment")
	// if value != "" {
	// 	t.Fatal("expected empty string")
	// 	t.FailNow()
	// }

	// value = loader.GetMetadataValueByType("test-event-one", "edge_type")
	// if value != "internal" {
	// 	logger.Info(value)
	// 	t.Fatal("expected value to be internal")
	// 	t.FailNow()
	// }
	loader.closer <- true
}

func TestRetryPull(t *testing.T) {
	stats, _ := monitoring.InitStats("scieng-test.ingester")
	_, err := NewMetadataLoader(
		&mockFetcher{
			failFetch: []bool{
				true,
				true,
				true,
				true,
				true,
			},
		},
		1*time.Second,
		1*time.Microsecond,
		stats,
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

/*
==================================
*/

// // MockS3Client is a mock S3 client
// type MockS3Client struct {
// 	s3iface.S3API
// }

// func (m *MockS3Client) GetObject(*s3.GetObjectInput) (*s3.GetObjectOutput, error) {
// 	return &s3.GetObjectOutput{}, nil
// }

// func TestOne(t *testing.T) {

// 	eventMetadataReloadFrequency := 5 * time.Minute
// 	eventMetadataRetryDelay := 2 * time.Second

// 	// session, err := session.NewSession()
// 	// if err != nil {
// 	// t.Error("Failed to start new session")
// 	// }
// 	// s3 := s3.New(session, aws.NewConfig().WithRegion("us-west-2"))
// 	fetcher := NewFetcher("science-blueprint-configs-integration", "fredcao-event-metadata-configs.json.gz", &MockS3Client{})

// 	stats, _ := monitoring.InitStats("scieng-test.ingester")
// 	eventMetadataLoader, err := NewMetadataLoader(
// 		fetcher, eventMetadataReloadFrequency,
// 		eventMetadataRetryDelay, stats)
// 	if err != nil {
// 		logger.WithError(err).Error("Failed S3")
// 	}

// 	logger.Info(eventMetadataLoader.GetMetadataValueByType("rare-event", "comment"))
// 	// logger.Go(eventMetadataLoader.Crank)

// 	return
// }

// var (
//  knownScoopProtocolConfig1 = []scoop_protocol.Config{
//      {
//          EventName: "foo",
//          Columns: []scoop_protocol.ColumnDefinition{
//              {
//                  InboundName:           "in",
//                  OutboundName:          "out",
//                  Transformer:           "int",
//                  ColumnCreationOptions: "",
//              },
//          },
//      },
//  }
//  knownScoopProtocolConfig2 = []scoop_protocol.Config{
//      {
//          EventName: "bar",
//          Columns: []scoop_protocol.ColumnDefinition{
//              {
//                  InboundName:           "in",
//                  OutboundName:          "out",
//                  Transformer:           "int",
//                  ColumnCreationOptions: "",
//              },
//          },
//      },
//  }
// )

// func TestRefresh(t *testing.T) {
//  c, _ := statsd.NewNoop()

//  dl, err := NewDynamicLoader(
//      &testFetcher{
//          failFetch: []bool{
//              false,
//              false,
//          },
//          configs: [][]scoop_protocol.Config{
//              knownScoopProtocolConfig1,
//              knownScoopProtocolConfig2,
//          },
//      },
//      1*time.Microsecond,
//      1,
//      reporter.WrapCactusStatter(c, 0.1),
//      nil,
//      geoip.Noop(),
//  )
//  if err != nil {
//      t.Fatalf("was expecting no error but got %v\n", err)
//      t.FailNow()

//  }
//  _, err = dl.GetColumnsForEvent("foo")
//  if err != nil {
//      t.Fatal("expected to track foo")
//      t.FailNow()
//  }

//  go dl.Crank()
//  time.Sleep(101 * time.Millisecond)
//  _, err = dl.GetColumnsForEvent("bar")
//  if err != nil {
//      t.Fatal("expected to track bar")
//      t.FailNow()
//  }
//  _, err = dl.GetColumnsForEvent("foo")
//  if err == nil {
//      t.Fatal("expected to not track foo")
//      t.FailNow()
//  }
//  dl.closer <- true
// }

// func TestRetryPull(t *testing.T) {
//  c, _ := statsd.NewNoop()
//  _, err := NewDynamicLoader(
//      &testFetcher{
//          failFetch: []bool{
//              true,
//              true,
//              true,
//              true,
//              true,
//          },
//      },
//      1*time.Second,
//      1*time.Microsecond,
//      reporter.WrapCactusStatter(c, 0.1),
//      nil,
//      geoip.Noop(),
//  )
//  if err == nil {
//      t.Fatalf("expected loader to timeout\n")
//      t.FailNow()
//  }
// }

// type testFetcher struct {
//  failFetch []bool
//  configs   [][]scoop_protocol.Config

//  i int
// }

// type testReadWriteCloser struct {
//  config []scoop_protocol.Config
// }

// func (trwc *testReadWriteCloser) Read(p []byte) (int, error) {
//  var b []byte
//  b, _ = json.Marshal(trwc.config)
//  return copy(p, b), io.EOF
// }

// func (trwc *testReadWriteCloser) Write(p []byte) (int, error) {
//  return len(p), nil
// }

// func (trwc *testReadWriteCloser) Close() error {
//  return nil
// }

// func (t *testFetcher) FetchAndWrite(r io.ReadCloser, w io.WriteCloser) error {
//  return nil
// }

// func (t *testFetcher) Fetch() (io.ReadCloser, error) {
//  if len(t.failFetch) < t.i && t.failFetch[t.i] {
//      t.i++
//      return nil, fmt.Errorf("failed on %d try", t.i)
//  }
//  if len(t.configs)-1 < t.i {
//      return nil, fmt.Errorf("failed on %d try", t.i)
//  }
//  rc := &testReadWriteCloser{
//      config: t.configs[t.i],
//  }
//  t.i++
//  return rc, nil
// }

// func (t *testFetcher) ConfigDestination(d string) (io.WriteCloser, error) {
//  return &testReadWriteCloser{
//      config: knownScoopProtocolConfig1,
//  }, nil
// }
