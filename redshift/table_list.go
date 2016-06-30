package redshift

import (
	"fmt"
	"time"
)

const (
	tableListCommand = `SELECT tablename FROM pg_tables WHERE schemaname = $1`
)

//TableListRequest is a request struct that requests the list of tables for a given schema
type TableListRequest struct {
	BuiltOn time.Time
	Schema  string
}

//GetStartTime returns the time when the request was constructed
func (r *TableListRequest) GetStartTime() time.Time {
	return r.BuiltOn
}

//GetCategory returns the sql execution category for the query
func (r *TableListRequest) GetCategory() string {
	return "Select"
}

//GetResult returns the result of executing a query
func (r *TableListRequest) GetResult(i int, err error) *RSResult {
	status := 0
	if err != nil {
		status = 1
	}
	return &RSResult{
		fmt.Sprintf("Updated %d rows with err: %v", i, err),
		status,
	}
}

//Query uses the RSConnection to execute the query created by the request
func (r *TableListRequest) Query(rs *RSConnection) ([]string, error) {
	var tables []string
	rows, err := rs.Conn.Query(tableListCommand, r.Schema)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, err
		}
		tables = append(tables, t)
	}
	return tables, nil
}
