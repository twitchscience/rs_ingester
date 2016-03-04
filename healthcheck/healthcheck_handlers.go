package healthcheck

import (
	"encoding/json"
	"net/http"

	"github.com/twitchscience/scoop_protocol/scoop_protocol"
	"github.com/zenazn/goji/web"
)

// Handler for healthcheck
type Handler struct {
	hcb *Backend
}

// IngesterHealthStatus represents the health status of scoop and the ingester
type IngesterHealthStatus struct {
	ScoopHealthCheckStatus    *scoop_protocol.ScoopHealthCheck
	ScoopHealthCheckConnError *string
	IngesterDBConnError       *string
}

// NewHealthCheckHandler creates a handler for the health check
func NewHealthCheckHandler(hcb *Backend) *Handler {
	return &Handler{hcb}
}

// HealthCheck responds with the health of the ingester
func (h *Handler) HealthCheck(c web.C, w http.ResponseWriter, r *http.Request) {
	ingesterStatus, responseCode := h.hcb.GetHealthStatus()

	js, err := json.Marshal(ingesterStatus)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(responseCode)
	_, err = w.Write(js)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
