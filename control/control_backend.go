package control

import (
	"fmt"

	"github.com/twitchscience/rs_ingester/metadata"
	"github.com/twitchscience/rs_ingester/migrator"
	"github.com/twitchscience/rs_ingester/versions"
)

// Backend is the backend for control, which operates on the ingester
type Backend struct {
	metaReader       metadata.Reader
	versions         versions.Getter
	versionIncrement chan migrator.VersionIncrement
}

// NewControlBackend instantiates the control backend with a db connection
func NewControlBackend(metaReader metadata.Reader, tableVersions versions.Getter,
	versionIncrement chan migrator.VersionIncrement) *Backend {
	return &Backend{metaReader, tableVersions, versionIncrement}
}

// ForceIngest makes the given table the highest priority to load next
func (cBackend *Backend) ForceIngest(tableName string) error {
	currentVersion, _ := cBackend.versions.Get(tableName)
	err := cBackend.metaReader.PrioritizeTSVVersion(tableName, currentVersion)
	if err != nil {
		return fmt.Errorf("Error executing query: %v", err)
	}
	return nil
}

// TableExists returns whether the given table name exists in our version dictionary.
func (cBackend *Backend) TableExists(tableName string) bool {
	_, exists := cBackend.versions.Get(tableName)
	return exists
}

// IncrementVersion increments the given table's version in the migrator goroutine.
func (cBackend *Backend) IncrementVersion(tableName string) error {
	errChan := make(chan error)
	curVersion, ok := cBackend.versions.Get(tableName)
	if !ok {
		curVersion = -1
	}
	version := curVersion + 1
	cBackend.versionIncrement <- migrator.VersionIncrement{
		Table: tableName, Version: version, Response: errChan,
	}
	err := <-errChan
	if err != nil {
		return fmt.Errorf("error setting table '%s' to version '%d': %v", tableName, version, err)
	}
	return nil
}
