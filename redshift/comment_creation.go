package redshift

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/lib/pq"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

const (
	createCommentCommand = `COMMENT ON TABLE %s IS '%s'`
)

//CreateTableCommentRequest holds the config of the table comments being created
type CreateTableCommentRequest struct {
	BuiltOn time.Time
	Config  *scoop_protocol.Config
}

//GetExec gets the executable sql for redshift
func (r *CreateTableCommentRequest) GetExec() string {
	serialized, err := json.Marshal(r.Config)
	if err != nil {
		serialized = []byte{}
	}
	return fmt.Sprintf(createCommentCommand,
		pq.QuoteIdentifier(r.Config.EventName),
		string(serialized))
}

//GetStartTime gets the time the request was built
func (r *CreateTableCommentRequest) GetStartTime() time.Time {
	return r.BuiltOn
}

//GetCategory returns the category of the sql operation taking place
func (r *CreateTableCommentRequest) GetCategory() string {
	return "Update"
}

//GetMessage returns the sql code from GetExec
func (r *CreateTableCommentRequest) GetMessage() string {
	return r.GetExec()
}

//GetResult formats the result as a success or not with number of rows updated
func (r *CreateTableCommentRequest) GetResult(i int, err error) *RSResult {
	status := 0
	if err != nil {
		status = 1
	}
	return &RSResult{
		fmt.Sprintf("Updated %d rows with err: %v", i, err),
		status,
	}
}
