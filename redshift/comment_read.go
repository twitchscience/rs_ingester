package redshift

import (
	"fmt"
	"time"
)

const (
	commentFetchCommand = `
SELECT obj_description(
  (SELECT p.oid from pg_class p
   JOIN pg_namespace n ON n.oid = p.relnamespace
   WHERE p.relname = '%s'
   AND n.nspname = '%s'),
'pg_class')
`
)

//ReadTableCommentRequest is a request struct that lets you see the schema comment on a redshift table
type ReadTableCommentRequest struct {
	BuiltOn time.Time
	Name    string
	Schema  string
}

//GetExec returns the sql code for the ReadTableCommentRequest
func (r *ReadTableCommentRequest) GetExec() string {
	return fmt.Sprintf(commentFetchCommand, r.Name, r.Schema)
}

//GetStartTime returns the start time of the redshift request
func (r *ReadTableCommentRequest) GetStartTime() time.Time {
	return r.BuiltOn
}

//GetCategory returns the sql execution category
func (r *ReadTableCommentRequest) GetCategory() string {
	return "Select"
}

//GetMessage returns the constructed sql query for ReadTableCommentRequest
func (r *ReadTableCommentRequest) GetMessage() string {
	return r.GetExec()
}

//GetResult returns the success status of the sql query
func (r *ReadTableCommentRequest) GetResult(i int, err error) *RSResult {
	status := 0
	if err != nil {
		status = 1
	}
	return &RSResult{
		fmt.Sprintf("Updated %d rows with err: %v", i, err),
		status,
	}
}

//Query runs the query provided the RS connection
func (r *ReadTableCommentRequest) Query(rs *RSConnection) (string, error) {
	var comment string
	err := rs.Conn.QueryRow(r.GetExec()).Scan(&comment)
	if err != nil {
		return "", err
	}
	return comment, nil
}
