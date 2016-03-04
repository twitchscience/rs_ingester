package loadclient

import (
	"github.com/twitchscience/rs_ingester/metadata"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

// LoadError is an error from a load
type LoadError interface {
	error
	Retryable() bool
}

// Loader interacts with scoop loads
type Loader interface {
	LoadManifest(manifest *metadata.LoadManifest) LoadError
	CheckLoad(manifestUUID string) (scoop_protocol.LoadStatus, error)
	PingScoopHealthcheck() (*scoop_protocol.ScoopHealthCheck, error)
}
