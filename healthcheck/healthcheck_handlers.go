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

type HealthStatus struct {
	ScoopHealthError                    *scoop_protocol.ConnError
	ScoopConnError, IngesterDBConnError *string
}

func BuildHealthCheckHandler(hcb *HealthCheckBackend) *HealthCheckHandler {
	b := &HealthCheckHandler{hcb}
	return b
}

func (h *HealthCheckHandler) HealthCheckPage(c web.C, w http.ResponseWriter, r *http.Request) {
	ingesterErr := h.hcb.HealthCheckIngesterDB()
	scoopStatus, scoopErr := h.hcb.HealthCheckScoop()

	responseCode := http.StatusOK

	var scoopHealthError *scoop_protocol.ConnError
	var scoopConnError, ingesterDBConnError string

	connStatus := HealthStatus{nil, nil, nil}

	scoopHealthError = scoopStatus
	connStatus.ScoopHealthError = scoopHealthError

	if scoopErr != nil {
		responseCode = http.StatusServiceUnavailable
		scoopConnError = scoopErr.Error()
		connStatus.ScoopConnError = &scoopConnError
	}
	if ingesterErr != nil {
		responseCode = http.StatusInternalServerError
		ingesterDBConnError = ingesterErr.Error()
		connStatus.IngesterDBConnError = &ingesterDBConnError
	}

	js, err := json.Marshal(connStatus)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	w.WriteHeader(responseCode)

	w.Write(js)
}
