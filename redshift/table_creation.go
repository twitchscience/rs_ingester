package redshift

import (
	"fmt"
	"time"

	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

const (
	createTableCommand = `CREATE TABLE "%s"%s;`
)

//TableCreateRequest is a struct that holds the request for a new table that needs to be created
type TableCreateRequest struct {
	BuiltOn time.Time
	Table   *scoop_protocol.Config
}

//GetExec returns constructed execution query provided values in the request
func (r *TableCreateRequest) GetExec() string {
	return fmt.Sprintf(createTableCommand,
		r.Table.EventName,
		r.Table.GetColumnCreationString())
}

//GetStartTime returns the time the request was constructed
func (r *TableCreateRequest) GetStartTime() time.Time {
	return r.BuiltOn
}

//GetCategory returns the appropriate sql execution category
func (r *TableCreateRequest) GetCategory() string {
	return "Create"
}

//GetMessage returns a string of the constructed query
func (r *TableCreateRequest) GetMessage() string {
	return r.GetExec()
}

//GetResult returns the result of executing the query
func (r *TableCreateRequest) GetResult(i int, err error) *RSResult {
	status := 0
	if err != nil {
		status = 1
	}
	return &RSResult{
		fmt.Sprintf("Created %d rows with err: %s", i, err),
		status,
	}
}
