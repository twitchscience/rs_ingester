package healthcheck

import (
	"net/http"

	_ "github.com/lib/pq" // To register "postgres" with database/sql
	"github.com/twitchscience/rs_ingester/loadclient"
	"github.com/twitchscience/rs_ingester/metadata"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

// Backend is the healthcheck's backend
type Backend struct {
	scoopConnection loadclient.Loader
	pgBackend       metadata.Backend
}

// NewHealthCheckBackend inits a new healthcheck backend
func NewHealthCheckBackend(scoopConnection loadclient.Loader, pgBackend metadata.Backend) *Backend {
	return &Backend{scoopConnection, pgBackend}
}

// GetHealthStatus checks if we can talk to scoop and the ingester db
func (hcBackend *Backend) GetHealthStatus() (IngesterHealthStatus, int) {
	scoopStatus, scoopErr := hcBackend.scoopConnection.PingScoopHealthcheck()
	ingesterErr := hcBackend.pgBackend.PingDB()

	responseCode := http.StatusOK

	var scoopHealthCheckStatus *scoop_protocol.ScoopHealthCheck
	var scoopHealthCheckConnError string
	var ingesterDBConnError string

	ingesterStatus := IngesterHealthStatus{nil, nil, nil}

	scoopHealthCheckStatus = scoopStatus
	ingesterStatus.ScoopHealthCheckStatus = scoopHealthCheckStatus

	if scoopErr != nil {
		responseCode = http.StatusServiceUnavailable
		scoopHealthCheckConnError = scoopErr.Error()
		ingesterStatus.ScoopHealthCheckConnError = &scoopHealthCheckConnError
	}
	if ingesterErr != nil {
		responseCode = http.StatusInternalServerError
		ingesterDBConnError = ingesterErr.Error()
		ingesterStatus.IngesterDBConnError = &ingesterDBConnError
	}

	return ingesterStatus, responseCode
}
