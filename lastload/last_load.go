package lastload

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/context"
	"github.com/twitchscience/rs_ingester/lib"
	"github.com/twitchscience/rs_ingester/metadata"
	"github.com/zenazn/goji/web"
	"github.com/zenazn/goji/web/middleware"
)

// Handler is a handler for last load information
type Handler struct {
	llManager metadata.LastLoadManager
}

// NewLastLoadHandler instantiates a handler for last load information
func NewLastLoadHandler(llManager metadata.LastLoadManager) *Handler {
	return &Handler{llManager}
}

// GetLastLoad is the handler for last load requests
func (h *Handler) GetLastLoad(c web.C, w http.ResponseWriter, r *http.Request) {
	lastload := h.llManager.GetLastLoads()

	llEpoch := map[string]int64{}
	for table, time := range lastload {
		llEpoch[table] = time.Unix()
	}

	js, err := json.Marshal(llEpoch)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(js)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// NewLastLoadRouter instantiates an http.Handler that serves last load information
func NewLastLoadRouter(llHandler *Handler) http.Handler {
	ll := web.New()

	ll.Use(middleware.EnvInit)
	ll.Use(middleware.RequestID)
	ll.Use(middleware.RealIP)
	ll.Use(lib.SimpleLogger)
	ll.Use(context.ClearHandler)

	ll.Get("/last_load", llHandler.GetLastLoad)

	return ll
}
