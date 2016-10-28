package control

import (
	"fmt"

	"github.com/twitchscience/rs_ingester/backend"
	"github.com/twitchscience/rs_ingester/metadata"
	"github.com/twitchscience/rs_ingester/versions"
)

// Backend is the backend for control, which operates on the ingester
type Backend struct {
	metaReader metadata.Reader
	versions   versions.Getter
	backend    backend.Backend
}

// NewControlBackend instantiates the control backend with a db connection
func NewControlBackend(metaReader metadata.Reader, tableVersions versions.Getter, backend backend.Backend) *Backend {
	return &Backend{metaReader, tableVersions, backend}
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
