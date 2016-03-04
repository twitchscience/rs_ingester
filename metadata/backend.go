package metadata

import "github.com/twitchscience/scoop_protocol/scoop_protocol"

// Load represents a file that needs to be loaded
type Load scoop_protocol.RowCopyRequest

// LoadManifest represents a set of files that needs to be loaded
type LoadManifest struct {
	Loads     []Load
	TableName string
	UUID      string
}

// Backend specifies the interface for load state
type Backend interface {
	Storer
	LoadReady() chan *LoadManifest
	LoadError(manifestUUID, loadError string)
	LoadDone(manifestUUID string)
	PingDB() error
}

// Storer specifies recording loads in the db
type Storer interface {
	InsertLoad(load *Load) error
	Close()
}
