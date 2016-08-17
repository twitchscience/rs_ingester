package metadata

import (
	"time"

	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

// Load represents a file that needs to be loaded
type Load scoop_protocol.RowCopyRequest

// LoadManifest represents a set of files that needs to be loaded
type LoadManifest struct {
	Loads     []Load
	TableName string
	UUID      string
}

// Reader specifies the interface for reading current table versions in tsvs
type Reader interface {
	Versions() (map[string]int, error)
	PingDB() error
	TSVVersionExists(table string, version int) (bool, error)
	PrioritizeTSVVersion(table string, version int) error
	GetPendingTables() ([]Event, error)
}

// Backend specifies the interface for load state
type Backend interface {
	Storer
	Reader
	LoadReady() chan *LoadManifest
	LoadError(manifestUUID, loadError string)
	LoadDone(manifestUUID string)
}

// Storer specifies recording loads in the db
type Storer interface {
	InsertLoad(load *Load) error
	Close()
}

// Event represents an event, or table, that needs to be loaded
type Event struct {
	Name      string
	Count     int
	Timestamp time.Time
}
