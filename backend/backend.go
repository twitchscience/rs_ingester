package backend

import "github.com/twitchscience/scoop_protocol/scoop_protocol"

//Backend is an interface that represents what operations on a DB must be available
type Backend interface {
	HealthCheck() error
	LoadCheck(*scoop_protocol.LoadCheckRequest) (*scoop_protocol.LoadCheckResponse, error)
	ManifestCopy(*scoop_protocol.ManifestRowCopyRequest) error
	TableVersions() (map[string]int, error)
	ApplyOperations(string, []scoop_protocol.Operation, []scoop_protocol.ColumnDefinition, int, int) error
	CreateTable(string, []scoop_protocol.Operation, []scoop_protocol.ColumnDefinition, int) error
	TableExists(string) (bool, error)
	TableLocked(string) (bool, error)
}
