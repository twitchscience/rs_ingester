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
	LoadManifest(manifest *metadata.LoadManifest) LoadError
	CheckLoad(manifestUuid string) (scoop_protocol.LoadStatus, error)
	PingScoopHealthcheck() (*scoop_protocol.ScoopHealthCheck, error)
}
