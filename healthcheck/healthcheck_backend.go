package healthcheck

import (
	"net/http"

	_ "github.com/lib/pq"
	"github.com/twitchscience/rs_ingester/loadclient"
	"github.com/twitchscience/rs_ingester/metadata"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

type HealthCheckBackend struct {
	scoopConnection loadclient.Loader
	pgBackend       metadata.MetadataBackend
}

func BuildHealthCheckBackend(scoopConnection loadclient.Loader, pgBackend metadata.MetadataBackend) *HealthCheckBackend {
	return &HealthCheckBackend{scoopConnection, pgBackend}
}

func (hcBackend *HealthCheckBackend) GetScoopHealthCheck() (*scoop_protocol.ScoopHealthCheck, error) {
	scoopStatus, err := hcBackend.scoopConnection.PingScoopHealthcheck()
	return scoopStatus, err
}

func (hcBackend *HealthCheckBackend) GetIngesterDBHealthCheck() error {
	err := hcBackend.pgBackend.PingDB()
	return err
}

func (hcBackend *HealthCheckBackend) GetHealthStatus() (IngesterHealthStatus, int) {
	scoopStatus, scoopErr := hcBackend.scoopConnection.PingScoopHealthcheck()
	ingesterErr := hcBackend.pgBackend.PingDB()

	responseCode := http.StatusOK

	var scoopHealthCheckStatus *scoop_protocol.ScoopHealthCheck
	var scoopHealthCheckConnError string
	var ingesterDBConnError string

	ingesterHealthStatus := IngesterHealthStatus{nil, nil, nil}

	scoopHealthCheckStatus = scoopStatus
	ingesterHealthStatus.ScoopHealthCheckStatus = scoopHealthCheckStatus

	if scoopErr != nil {
		responseCode = http.StatusServiceUnavailable
		scoopHealthCheckConnError = scoopErr.Error()
		ingesterHealthStatus.ScoopHealthCheckConnError = &scoopHealthCheckConnError
	}
	if ingesterErr != nil {
		responseCode = http.StatusInternalServerError
		ingesterDBConnError = ingesterErr.Error()
		ingesterHealthStatus.IngesterDBConnError = &ingesterDBConnError
	}

	return ingesterHealthStatus, responseCode
}
