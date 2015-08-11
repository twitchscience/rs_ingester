package loadclient

import (
	"github.com/twitchscience/rs_ingester/metadata"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

type TestLoader struct {
}

func NewTestLoader() Loader {
	return &TestLoader{}
}

func (t *TestLoader) LoadBatch(batch *metadata.LoadBatch) LoadError {
	return nil
}

func (t *TestLoader) CheckLoad(batchUuid string) (scoop_protocol.LoadStatus, error) {
	return scoop_protocol.LoadComplete, nil
}

func (t *TestLoader) PingScoopHealthcheck() (*scoop_protocol.ConnError, error) {
	return nil, nil
}
