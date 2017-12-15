package control

import (
	"encoding/json"
	"net/http"

	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/aws_utils/monitoring"
	"github.com/zenazn/goji/web"
)

// Handler is a handler for control
type Handler struct {
	cb    *Backend
	stats monitoring.SafeStatter
}

// NewControlHandler instantiates a handler for control
func NewControlHandler(ch *Backend, stats monitoring.SafeStatter) *Handler {
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

// ForceLoad forces ingest of a particular table. Takes a JSON POST containing the
// Table and Requester fields, representing the what to force load and who wants it.
func (ch *Handler) ForceLoad(c web.C, w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	var tableArg struct {
		Table     string
		Requester string
	}
	err := decoder.Decode(&tableArg)
	if err != nil {
		respondWithJSONError(w, "Problem decoding JSON POST data.", http.StatusBadRequest)
		return
	}

	if len(tableArg.Table) <= 0 {
		respondWithJSONError(w, "Table name empty.", http.StatusBadRequest)
		return
	}

	err = ch.cb.ForceLoad(tableArg.Table, tableArg.Requester)
	if err != nil {
		logger.WithError(err).WithField("table", tableArg.Table).
			WithField("requester", tableArg.Requester).Error("Error forcing load")
		respondWithJSONError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	ch.stats.SafeInc("force_load."+tableArg.Table, 1, 1.0)
	w.WriteHeader(http.StatusNoContent)
}

// TableExists returns a boolean indicating whether the given table exists.
func (ch *Handler) TableExists(c web.C, w http.ResponseWriter, r *http.Request) {
	table := c.URLParams["id"]

	exists := ch.cb.TableExists(table)
	js, err := json.Marshal(struct{ Exists bool }{exists})
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

// IncrementVersion sets the table's version in infra.table_version to the given version.
func (ch *Handler) IncrementVersion(c web.C, w http.ResponseWriter, r *http.Request) {
	table := c.URLParams["id"]

	err := ch.cb.IncrementVersion(table)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// LastLoad returns a JSON map of known last load times for each table
func (ch *Handler) LastLoad(c web.C, w http.ResponseWriter, r *http.Request) {
	lastloads := ch.cb.LastLoads()

	llEpoch := map[string]int64{}
	for table, time := range lastloads {
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
