package loadclient

import (
	"github.com/twitchscience/rs_ingester/metadata"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

type LoadError interface {
	error
	Retryable() bool
}

type Loader interface {
	LoadBatch(batch *metadata.LoadBatch) LoadError
	CheckLoad(batchUuid string) (scoop_protocol.LoadStatus, error)
	PingScoopHealthcheck() (*scoop_protocol.ConnError, error)
}
