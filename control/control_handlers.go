package control

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/cactus/go-statsd-client/statsd"
	"github.com/zenazn/goji/web"
)

type ControlHandler struct {
	cb    *ControlBackend
	stats statsd.Statter
}

type ControlError struct {
	Error string
}

func NewControlHandler(ch *ControlBackend, stats statsd.Statter) *ControlHandler {
	return &ControlHandler{ch, stats}
}

func respondWithJsonError(w http.ResponseWriter, text string, responseCode int) {
	/*
		respondWithJsonError esponds with a JSON error with the given error code. The format of the
		JSON error is {"Error": text}

		It's very likely that you want to return from the handler after calling
		this.
	*/

	js, err := json.Marshal(ControlError{text})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(responseCode)
	w.Write(js)
}

func (ch *ControlHandler) ForceIngest(c web.C, w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	table := r.Form.Get("table")

	if len(table) <= 0 {
		respondWithJsonError(w, "Table name empty.", http.StatusBadRequest)
		return
	}

	err := ch.cb.ForceIngest(table)
	if err != nil {
		respondWithJsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	ch.stats.Inc("force_ingest."+table, 1, 1.0) // TODO: include table name?
	w.WriteHeader(http.StatusNoContent)
}

func (ch *ControlHandler) GetPendingTables(c web.C, w http.ResponseWriter, r *http.Request) {
	pendingTables, err := ch.cb.GetPendingTables()
	if err != nil {
		respondWithJsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	js, err := json.Marshal(pendingTables)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	ch.stats.Inc("get_pending_tables", 1, 1.0)
	log.Printf("Retrieved pending tables: %v.\n", pendingTables)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(js)
}
