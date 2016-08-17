package control

import (
	"fmt"

	"github.com/twitchscience/rs_ingester/metadata"
	"github.com/twitchscience/rs_ingester/versions"
)

// Backend is the backend for control, which operates on the ingester
type Backend struct {
	metaReader metadata.Reader
	versions   versions.Getter
}

// NewControlBackend instantiates the control backend with a db connection
func NewControlBackend(metaReader metadata.Reader, tableVersions versions.Getter) *Backend {
	return &Backend{metaReader, tableVersions}
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

// GetPendingTables returns the list of tables with loads currently pending
func (cBackend *Backend) GetPendingTables() ([]metadata.Event, error) {
	return cBackend.metaReader.GetPendingTables()
}
