package healthcheck

import (
	"net/http"

	_ "github.com/lib/pq" // To register "postgres" with database/sql
	"github.com/twitchscience/rs_ingester/loadclient"
	"github.com/twitchscience/rs_ingester/metadata"
)

//Backend is the backend for the health check
type Backend struct {
	connection loadclient.Loader
	pgBackend  metadata.Reader
}

// NewBackend inits a new healthcheck backend
func NewBackend(connection loadclient.Loader, pgBackend metadata.Reader) *Backend {
	return &Backend{connection, pgBackend}
}

//HealthStatus returns the health status of the ingesters dependencies
func (b *Backend) HealthStatus() (IngesterHealthStatus, int) {
	redshiftErr := b.connection.HealthCheck()
	ingesterErr := b.pgBackend.PingDB()

	responseCode := http.StatusOK

	var redshiftDBConnError string
	var ingesterDBConnError string

	ingesterHealthStatus := IngesterHealthStatus{nil, nil}

	if redshiftErr != nil {
		redshiftDBConnError = redshiftErr.Error()
		ingesterHealthStatus.RedshiftDBConnError = &redshiftDBConnError
	}
	if ingesterErr != nil {
		ingesterDBConnError = ingesterErr.Error()
		ingesterHealthStatus.IngesterDBConnError = &ingesterDBConnError
	}

	return ingesterHealthStatus, responseCode
}
