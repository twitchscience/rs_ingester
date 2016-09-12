package backend

import (
	"github.com/twitchscience/rs_ingester/redshift"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

//Backend is an interface that represents what operations on a DB must be available
type Backend interface {
	HealthCheck() error
	Copy(*scoop_protocol.RowCopyRequest) error
	LoadCheck(*scoop_protocol.LoadCheckRequest) (*scoop_protocol.LoadCheckResponse, error)
	ManifestCopy(*scoop_protocol.ManifestRowCopyRequest) error
	Query(*redshift.QueryRequest) ([]byte, error)
	AllSchemas() ([]scoop_protocol.Config, error)
	TableVersions() (map[string]int, error)
	Schema(event string) (*scoop_protocol.Config, error)
	NewUser(user, pw string) error
	UpdatePassword(user, pw string) error
	UpdateGroup(user, group string) error
	MakeSuperuser(user string) error
	EnforcePermissions() error
	ApplyOperations(string, []scoop_protocol.Operation, int) error
	CreateTable(string, []scoop_protocol.Operation) error
}
