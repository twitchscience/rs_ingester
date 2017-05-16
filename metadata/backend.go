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

// Reader specifies the interface for Backend read/write operations
type Reader interface {
	Versions() (map[string]int, error)
	PingDB() error
	TSVVersionExists(table string, version int) (bool, error)
	ForceLoad(table string, requester string) error
	StatsForPendingLoads() ([]*PendingLoadStats, error)
	IsForceLoadRequested(table string) (bool, error)
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

// EventStats defines a set of statistics recorded for a particular event.
type EventStats struct {
	Event string
	Count int64
	MinTS time.Time
}

// PendingLoadType specifies a particular reason for events to be pending load.
type PendingLoadType string

const (
	// PendingInQueue are loads waiting in active queue.
	PendingInQueue PendingLoadType = "in_queue"

	// PendingStale are loads retried a maximum amount of times.
	PendingStale PendingLoadType = "stale"

	// PendingMigration are loads blocked on version migration.
	PendingMigration PendingLoadType = "pending_migration"
)

// PendingLoadStats stores aggregated stats for events in a particular type of pending load group.
type PendingLoadStats struct {
	Type  PendingLoadType
	Stats []*EventStats
}
