package redshift

import (
	"encoding/json"
	"fmt"
	"time"
)

//QueryRequest is a request struct that lets you perform an arbitrary query
type QueryRequest struct {
	BuiltOn         time.Time
	Query           string
	MaxRowsReturned int
}

//GetExec returns the query provided in the queryRequest
func (r *QueryRequest) GetExec() string {
	return r.Query
}

//GetStartTime returns the time the request was built
func (r *QueryRequest) GetStartTime() time.Time {
	return r.BuiltOn
}

//GetCategory returns the sql execution category of the query
func (r *QueryRequest) GetCategory() string {
	return "Query"
}

//GetMessage returns the string of the query being executed
func (r *QueryRequest) GetMessage() string {
	return r.GetExec()
}

//GetResult returns the query execution result of provided query in QueryRequest
func (r *QueryRequest) GetResult(i int, err error) *RSResult {
	status := 0
	if err != nil {
		status = 1
	}
	return &RSResult{
		fmt.Sprintf("Fetched %d rows with err: %v", i, err),
		status,
	}
}

//Exec returns the expected return value of the query provided in QueryRequest returned from redshift
func (r *QueryRequest) Exec(rs *RSConnection) ([]byte, error) {
	// TODO change to a Tx and add a timeout to abort
	rows, err := rs.Conn.Query(r.GetExec())
	if err != nil {
		return nil, err
	}
	resultTable := rs.mungeTable(rows, r.MaxRowsReturned, r.GetStartTime())
	b, err := json.Marshal(resultTable)
	return b, err
}
