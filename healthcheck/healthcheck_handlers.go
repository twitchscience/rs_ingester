package healthcheck

import (
	"encoding/json"
	"net/http"

	"github.com/twitchscience/scoop_protocol/scoop_protocol"
	"github.com/zenazn/goji/web"
)

type HealthCheckHandler struct {
	hcb *HealthCheckBackend
}

type IngesterHealthStatus struct {
	ScoopHealthCheckStatus    *scoop_protocol.ScoopHealthCheck
	ScoopHealthCheckConnError *string
	IngesterDBConnError       *string
}

func BuildHealthCheckHandler(hcb *HealthCheckBackend) *HealthCheckHandler {
	b := &HealthCheckHandler{hcb}
	return b
}

func (h *HealthCheckHandler) HealthCheckPage(c web.C, w http.ResponseWriter, r *http.Request) {
	ingesterErr := h.hcb.GetIngesterDBHealthCheck()
	scoopStatus, scoopErr := h.hcb.GetScoopHealthCheck()

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

	js, err := json.Marshal(ingesterHealthStatus)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	w.WriteHeader(responseCode)

	w.Write(js)
}
