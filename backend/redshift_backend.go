package backend

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/twitchscience/rs_ingester/redshift"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

//RedshiftBackend is the struct that holds the RSConnection pool and where backend operations are done from
type RedshiftBackend struct {
	connection  *redshift.RSConnection
	credentials *credentials.Credentials
}

//BuildRedshiftBackend builds a new redshift backend by also creating a new rsConnection
func BuildRedshiftBackend(credentials *credentials.Credentials, poolSize int, rsURL string) (*RedshiftBackend, error) {
	conn, err := redshift.BuildRSConnection(rsURL, poolSize)
	if err != nil {
		return nil, err
	}
	for i := 0; i < 5; i++ {
		go conn.Listen()
	}
	return &RedshiftBackend{
		connection:  conn,
		credentials: credentials,
	}, nil
}

//HealthCheck makes sure that that redshift is reachable
func (r *RedshiftBackend) HealthCheck() error {
	err := r.connection.Conn.Ping()
	return err
}

//Create makes a TableCreateRequest and returns the transaction
func (r *RedshiftBackend) Create(cfg *scoop_protocol.Config) error {
	createTable := &redshift.TableCreateRequest{
		BuiltOn: time.Now(),
		Table:   cfg,
	}
	createComment := &redshift.CreateTableCommentRequest{
		BuiltOn: time.Now(),
		Config:  cfg,
	}
	return r.connection.ExecInTransaction(createTable, createComment)
}

//Copy makes a RowCopyRequest and executes the request
func (r *RedshiftBackend) Copy(rc *scoop_protocol.RowCopyRequest) error {
	return r.connection.ExecFnInTransaction(redshift.RowCopyRequest{
		BuiltOn:     time.Now(),
		Name:        rc.TableName,
		Key:         rc.KeyName,
		Credentials: redshift.CopyCredentials(r.credentials),
	}.TxExec)
}

//ManifestCopy makes a ManifestRowCopyRequest and returns the function that executes the request
func (r *RedshiftBackend) ManifestCopy(rc *scoop_protocol.ManifestRowCopyRequest) error {
	return r.connection.ExecFnInTransaction(redshift.ManifestRowCopyRequest{
		BuiltOn:     time.Now(),
		Name:        rc.TableName,
		ManifestURL: rc.ManifestURL,
		Credentials: redshift.CopyCredentials(r.credentials),
	}.TxExec)
}

//LoadCheck makes a LoadCheckRequest and returns the response of the load check
func (r *RedshiftBackend) LoadCheck(req *scoop_protocol.LoadCheckRequest) (*scoop_protocol.LoadCheckResponse, error) {
	resp := &scoop_protocol.LoadCheckResponse{ManifestURL: req.ManifestURL}
	err := r.connection.ExecFnInTransaction(func(t *sql.Tx) (err error) {
		resp.LoadStatus, err = redshift.CheckLoadStatus(t, req.ManifestURL)
		return
	})
	return resp, err
}

func performColumnCheck(current, additions *scoop_protocol.Config) error {
	for _, col := range current.Columns {
		for _, add := range additions.Columns {
			if col.OutboundName == add.OutboundName {
				return fmt.Errorf("Event: %s already has Property: %s", current.EventName, col.OutboundName)
			}
		}
	}
	return nil
}

//Update runs the update table operation on redshift
func (r *RedshiftBackend) Update(additions *scoop_protocol.Config) error {
	return r.connection.ExecFnInTransaction(func(tx *sql.Tx) error {
		currentCfg, err := r.Schema(additions.EventName)
		if err != nil {
			return err
		}

		if err := performColumnCheck(currentCfg, additions); err != nil {
			return err
		}

		// preflight checks good, now alter table
		updateTable := &redshift.TableAlterRequest{
			TableName: additions.EventName,
			Additions: additions.Columns,
		}
		for _, query := range updateTable.ProduceQueries() {
			if _, err := tx.Exec(query); err != nil {
				return err
			}
		}

		// update comment
		currentCfg.Columns = append(currentCfg.Columns, additions.Columns...)
		createComment := &redshift.CreateTableCommentRequest{
			Config: currentCfg,
		}
		if _, err := tx.Exec(createComment.GetExec()); err != nil {
			return err
		}
		return nil
	})
}

//Query allows for the execution of an arbitrary QueryRequest
func (r *RedshiftBackend) Query(req *redshift.QueryRequest) ([]byte, error) {
	return req.Exec(r.connection)
}

//AllSchemas returns a list of all table schemas in the logs schema in redshift
func (r *RedshiftBackend) AllSchemas() ([]scoop_protocol.Config, error) {
	req := &redshift.TableListRequest{
		BuiltOn: time.Now(),
		Schema:  "logs",
	}
	tables, err := req.Query(r.connection)
	if err != nil {
		return nil, err
	}
	schemas := make([]scoop_protocol.Config, len(tables))
	for i, t := range tables {
		s, err := r.Schema(t)
		if err != nil {
			return nil, err
		}
		schemas[i] = *s
	}
	return schemas, nil
}

//Schema returns a specific table schema in the logs schema in redshift
func (r *RedshiftBackend) Schema(event string) (*scoop_protocol.Config, error) {
	req := &redshift.ReadTableCommentRequest{
		BuiltOn: time.Now(),
		Name:    event,
		Schema:  "logs",
	}
	comment, err := req.Query(r.connection)
	if err != nil {
		log.Println("Error reading comment on table:", event)
		return nil, err
	}
	var cfg scoop_protocol.Config
	err = json.Unmarshal([]byte(comment), &cfg)
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}

//NewUser returns a function that executes a new uesr operation on redshift
func (r *RedshiftBackend) NewUser(user, pw string) error {
	return r.connection.ExecFnInTransaction((&redshift.NewUser{
		User:     user,
		Password: pw,
	}).TxExec)
}

//UpdatePassword returns a function that executes an UpdatePassword operation on redshift
func (r *RedshiftBackend) UpdatePassword(user, pw string) error {
	return r.connection.ExecFnInTransaction((&redshift.UpdatePassword{
		User:     user,
		Password: pw,
	}).TxExec)
}

//MakeSuperuser returns a function that executes a make super user operation on redshift
func (r *RedshiftBackend) MakeSuperuser(user string) error {
	return r.connection.ExecFnInTransaction((&redshift.MakeSuperuser{
		User: user,
	}).TxExec)
}

//UpdateGroup returns a function that executes an operation that updates a group to add a new user, on redshift
func (r *RedshiftBackend) UpdateGroup(user, group string) error {
	return r.connection.ExecFnInTransaction((&redshift.UpdateGroup{
		User:  user,
		Group: group,
	}).TxExec)
}

//EnforcePermissions returns a function that repairs permissions on all tables on redshift
func (r *RedshiftBackend) EnforcePermissions() error {
	return r.connection.ExecFnInTransaction((&redshift.EnforcePerms{}).TxExec)
}
