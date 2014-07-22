package main

import (
	"testing"

	"github.com/twitchscience/scoop_protocol/scoop_protocol"
	"github.com/cactus/go-statsd-client/statsd"
	"github.com/crowdmob/goamz/sqs"
)

func TestHandleSimpleMessage(t *testing.T) {
	t.Skip("Requires a debug scoop server")

	fakeMsg := `{"KeyName": "foo", "TableName": "bar"}`

	testMsg := sqs.Message{Body: fakeMsg}

	stats, _ := statsd.NewNoop(0, nil)
	handler := IngestHandler{
		Auth:    scoop_protocol.GetScoopSigner(),
		Statter: stats,
	}

	if err := handler.Handle(&testMsg); err != nil {
		t.Errorf("Error '%s' was not expected while handling the test message", err)
	}
}
