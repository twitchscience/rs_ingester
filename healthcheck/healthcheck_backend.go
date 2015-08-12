package healthcheck

import (
	_ "github.com/lib/pq"
	"github.com/twitchscience/rs_ingester/loadclient"
	"github.com/twitchscience/rs_ingester/metadata"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

type HealthCheckBackend struct {
	postgresScoopConnection loadclient.Loader
	postgresBackend         metadata.MetadataBackend
}

func BuildHealthCheckBackend(postgresScoopConnection loadclient.Loader, postgresBackend metadata.MetadataBackend) *HealthCheckBackend {
	return &HealthCheckBackend{postgresScoopConnection, postgresBackend}
}

func (hcb *HealthCheckBackend) GetScoopHealthCheck() (*scoop_protocol.ScoopHealthCheck, error) {
	scoopStatus, err := hcb.postgresScoopConnection.PingScoopHealthcheck()
	return scoopStatus, err
}

func (hcb *HealthCheckBackend) GetIngesterDBHealthCheck() error {
	err := hcb.postgresBackend.PingDB()
	return err
}
