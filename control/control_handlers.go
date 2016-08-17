package control

import (
	"encoding/json"
	"net/http"

	"github.com/cactus/go-statsd-client/statsd"
	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/rs_ingester/metadata"
	"github.com/zenazn/goji/web"
)

// Handler is a handler for control
type Handler struct {
	cb    *Backend
	stats statsd.Statter
}

// NewControlHandler instantiates a handler for control
func NewControlHandler(ch *Backend, stats statsd.Statter) *Handler {
	return &Handler{ch, stats}
}

// respondWithJSONError responds with a JSON error with the given error code. The format of the
// JSON error is {"Error": text}
//	It's very likely that you want to return from the handler after calling
//	this.
func respondWithJSONError(w http.ResponseWriter, text string, responseCode int) {
	js, err := json.Marshal(struct{ Error string }{text})
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

// ForceIngest forces ingest of a particular table. Takes a JSON POST containing the
// table field, representing the name of the table to be ingested.
func (ch *Handler) ForceIngest(c web.C, w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	var tableArg struct {
		Table string
	}
	err := decoder.Decode(&tableArg)
	if err != nil {
		respondWithJSONError(w, "Problem decoding JSON POST data.", http.StatusBadRequest)
		return
	}
	table := tableArg.Table

	if len(table) <= 0 {
		respondWithJSONError(w, "Table name empty.", http.StatusBadRequest)
		return
	}

	err = ch.cb.ForceIngest(table)
	if err != nil {
		respondWithJSONError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	err = ch.stats.Inc("force_ingest."+table, 1, 1.0)
	if err != nil {
		logger.WithError(err).Printf("Error sending force_ingest message to statsd")
	}
	w.WriteHeader(http.StatusNoContent)
}

// GetPendingTables responds with the list of tables, or events, that are up to
// be loaded
func (ch *Handler) GetPendingTables(c web.C, w http.ResponseWriter, r *http.Request) {
	pendingTables, err := ch.cb.GetPendingTables()
	if err != nil {
		respondWithJSONError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	js, err := json.Marshal(struct{ Events []metadata.Event }{pendingTables})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	err = ch.stats.Inc("get_pending_tables", 1, 1.0)
	if err != nil {
		logger.WithError(err).Error("Error sending get_pending_tables message to statsd")
	}
	logger.WithField("pendingTables", pendingTables).Infof("Retrieved pending tables")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(js)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
