package redshift

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/lib/pq" //necessary for the postgres querys ran from funcs here
	"github.com/twitchscience/aws_utils/logger"
)

//Table is the internal representation of the the table in the rs_adaptor
type Table struct {
	Rows      [][]interface{} `json:"rows"`
	Columns   []string        `json:"columns"`
	TimeTaken time.Duration   `json:"timeTaken"`
	Err       error           `json:"err"`
}

//RSRequest is the interface that defines all query creations on redshift
type RSRequest interface {
	GetExec() string
	GetStartTime() time.Time
	GetCategory() string
	GetMessage() string
	GetResult(i int, err error) *RSResult
}

//RSConnection holds the actual connection to the redshift table
type RSConnection struct {
	Conn            *sql.DB
	InboundRequests chan RSRequest
}

//RSResult represents the response from redshift after a query is run
type RSResult struct {
	ResultMessage string
	Status        int
}

//GetStatusCode returns the status code of a RSResult
func (r *RSResult) GetStatusCode() int {
	return r.Status
}

//GetResultMessage returns the result message of RSResult
func (r *RSResult) GetResultMessage() string {
	return r.ResultMessage
}

//BuildRSConnection builds and returns a new connection to redshift
func BuildRSConnection(pgConnect string, maxOpenConnections int) (*RSConnection, error) {
	db, err := sql.Open("postgres", pgConnect)
	if err != nil {
		return nil, fmt.Errorf("Got err %v while connecting to db", err)
	}
	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("Could not ping the db %v", err)
	}
	db.SetMaxOpenConns(maxOpenConnections)
	return &RSConnection{
		Conn:            db,
		InboundRequests: make(chan RSRequest, 10),
	}, nil
}

func (rs *RSConnection) mungeTable(rows *sql.Rows, maxRowsReturned int, start time.Time) *Table {
	t := new(Table)
	cols, err := rows.Columns()
	t.Columns = cols
	if err != nil {
		return &Table{
			Err: err,
		}
	}

	// Make a slice for the values
	values := make([]*interface{}, len(t.Columns))

	// rows.Scan wants '[]interface{}' as an argument, so we must copy the
	// references into such a slice
	// See http://code.google.com/p/go-wiki/wiki/InterfaceSlice for details
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	rowsScanned := 0
	for rows.Next() {
		if rowsScanned > maxRowsReturned {
			break
		}

		err := rows.Scan(scanArgs...)
		if err != nil {
			return &Table{
				Err: err,
			}
		}
		row := make([]interface{}, len(t.Columns))
		for i, col := range values {
			if col == nil {
				row[i] = nil
			} else {
				switch (*col).(type) {
				case []byte:
					row[i] = string((*col).([]byte))
				default:
					row[i] = *col
				}
			}
		}
		t.Rows = append(t.Rows, row)
		rowsScanned++
	}
	t.TimeTaken = time.Since(start)
	defer func() {
		err := rows.Close()
		if err != nil {
			logger.WithError(err).Error("Could not close rows object")
		}
	}()
	return t
}

//Listen continuously listens on inbound requests to exec on the RSconnection
func (rs *RSConnection) Listen() {
	for req := range rs.InboundRequests {
		// only /query currently uses this
		_, _ = rs.ExecCommand(req)
	}
}

//ExecCommand is called by listen to initiate a transaction for a command for a RSRequest
func (rs *RSConnection) ExecCommand(r RSRequest) (int, error) {
	tx, err := rs.Conn.Begin()
	if err != nil {
		return 0, err
	}
	res, err := tx.Exec(r.GetExec())
	if err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			logger.WithError(rollbackErr).Error("Could not rollback successfully")
		}
		return 0, err
	}
	err = tx.Commit()
	if err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			logger.WithError(rollbackErr).Error("Could not rollback successfully")
		}
		return 0, err
	}
	nRows, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int(nRows), nil
}

//ExecInTransaction listens to multiple RSRequests and executes them in a single transaction
func (rs *RSConnection) ExecInTransaction(cmds ...RSRequest) error {
	tx, err := rs.Conn.Begin()
	if err != nil {
		return err
	}
	for _, cmd := range cmds {
		s := cmd.GetExec()
		logger.Info("Executing:", s)
		_, err = tx.Exec(s)
		if err != nil {
			logger.WithError(err).WithField("query", s).Error("Failed to execute")
			rollbackErr := tx.Rollback()
			if rollbackErr != nil {
				logger.WithError(rollbackErr).Error("Could not rollback successfully")
			}
			return err
		}
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

//ExecFnInTransaction takes a closure function of a request and runs it on redshift in a transaction
func (rs *RSConnection) ExecFnInTransaction(work func(*sql.Tx) error) error {
	tx, err := rs.Conn.Begin()
	if err != nil {
		return err
	}
	err = work(tx)
	if err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			logger.WithError(rollbackErr).Error("Could not rollback successfully")
		}
		return err
	}
	return tx.Commit()
}

// EscapePGString is a poor attempt to escape strings in postgres
// Prefer using $1 syntax with additional argument to Exec when possible
func EscapePGString(s string) string {
	a := strings.Replace(s, `\`, `\\`, -1)
	return "'" + strings.Replace(a, `'`, `''`, -1) + "'"
}
