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
	return &HealthCheckHandler{hcb}
}

func (h *HealthCheckHandler) HealthCheck(c web.C, w http.ResponseWriter, r *http.Request) {
	ingesterHealthStatus, responseCode := h.hcb.GetHealthStatus()

	js, err := json.Marshal(ingesterHealthStatus)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(responseCode)
	w.Write(js)
}
