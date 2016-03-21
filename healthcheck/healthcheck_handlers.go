package healthcheck

import (
	"encoding/json"
	"net/http"

	"github.com/zenazn/goji/web"
)

// Handler for healthcheck
type Handler struct {
	hcb *Backend
}

// IngesterHealthStatus represents the health status of scoop and the ingester
type IngesterHealthStatus struct {
	RedshiftDBConnError *string
	IngesterDBConnError *string
}

// NewHandler creates a handler for the health check
func NewHandler(hcb *Backend) *Handler {
	return &Handler{hcb}
}

// HealthCheck responds with the health of the ingester
func (h *Handler) HealthCheck(c web.C, w http.ResponseWriter, r *http.Request) {
	ingesterHealthStatus, responseCode := h.hcb.HealthStatus()

	js, err := json.Marshal(ingesterHealthStatus)
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
