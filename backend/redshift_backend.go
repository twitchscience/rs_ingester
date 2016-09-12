package backend

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/lib/pq"
	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/rs_ingester/redshift"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

var (
	transformerTypeMap = map[string]string{
		"ipCity":       "varchar(64)",
		"ipCountry":    "varchar(2)",
		"ipRegion":     "varchar(64)",
		"ipAsn":        "varchar(128)",
		"ipAsnInteger": "int",
		"f@timestamp":  "datetime",
	}
)

//RedshiftBackend is the struct that holds the RSConnection pool and where backend operations are done from
type RedshiftBackend struct {
	connection  *redshift.RSConnection
	credentials *credentials.Credentials
	tableLocks  map[string]*sync.Mutex
}

func buildTableLocks(conn *redshift.RSConnection) (map[string]*sync.Mutex, error) {
	locks := make(map[string]*sync.Mutex)
	currentTableVersions, err := getTableVersions(conn)
	if err != nil {
		return nil, err
	}
	for table := range currentTableVersions {
		locks[table] = &sync.Mutex{}
		logger.Info("Created %s Lock", table)
	}
	return locks, nil
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
	tableLocks, err := buildTableLocks(conn)
	if err != nil {
		return nil, err
	}
	return &RedshiftBackend{
		connection:  conn,
		credentials: credentials,
		tableLocks:  tableLocks,
	}, nil
}

//HealthCheck makes sure that that redshift is reachable
func (r *RedshiftBackend) HealthCheck() error {
	err := r.connection.Conn.Ping()
	return err
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
	lock, exist := r.tableLocks[rc.TableName]
	if !exist {
		return fmt.Errorf("Lock for %s did not exist", rc.TableName)
	}
	lock.Lock()
	defer lock.Unlock()

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
		logger.WithError(err).Error("Error reading comment on table")
		return nil, err
	}
	var cfg scoop_protocol.Config
	err = json.Unmarshal([]byte(comment), &cfg)
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}

func getTableVersions(conn *redshift.RSConnection) (map[string]int, error) {
	versions := make(map[string]int)
	rows, err := conn.Conn.Query(`SELECT name, MAX(version) FROM infra.table_version GROUP BY name;`)
	if err != nil {
		return nil, fmt.Errorf("Error SELECTing the table versions from ace's infra.table_version: %v", err)
	}
	defer func() {
		err = rows.Close()
		if err != nil {
			logger.WithError(err).Error("Error closing rows")
		}
	}()
	for rows.Next() {
		var table string
		var version int
		if err := rows.Scan(&table, &version); err != nil {
			return nil, err
		}
		versions[table] = version
	}
	return versions, nil
}

// TableVersions returns the event tables with version numbers
func (r *RedshiftBackend) TableVersions() (map[string]int, error) {
	return getTableVersions(r.connection)
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

type migrationStep scoop_protocol.Operation

func parseFunctionalType(s string) (string, bool) {
	if len(s) > 0 && s[0] == 'f' && s[1] == '@' {
		transformerType, ok := transformerTypeMap[s[:strings.LastIndex(s, "@")]]
		return transformerType, ok
	}
	return "", false
}

func (m *migrationStep) getCreationForm() string {
	tranType, isTranslated := transformerTypeMap[m.ActionMetadata["column_type"]]
	funcType, isFunc := parseFunctionalType(m.ActionMetadata["column_type"])

	var colType string
	if isTranslated {
		colType = tranType
	} else if isFunc {
		colType = funcType
	} else {
		colType = m.ActionMetadata["column_type"]
	}

	maybeColOpts := ""
	if len(m.ActionMetadata["column_options"]) > 1 {
		maybeColOpts = m.ActionMetadata["column_options"]
	}

	return fmt.Sprintf("%s %s%s", pq.QuoteIdentifier(m.Name), colType, maybeColOpts)
}

// expectVersion checks to see if the version in infra.table_version is what was
// given. Special case for version=-1 means you expect table doesn't exist
func expectVersion(tx *sql.Tx, table string, version int) error {
	var readVersion int
	err := tx.QueryRow(`SELECT MAX(version) FROM infra.table_version WHERE name = $1 GROUP BY name;`, table).Scan(&readVersion)
	switch {
	case err == sql.ErrNoRows:
		if version == -1 {
			return nil
		}
		return fmt.Errorf("Expected version %d for table %s, but table doesn't exist in infra.table_version.", version, table)
	case err != nil:
		return fmt.Errorf("Error finding table version from ace: %v", err)
	default:
		if readVersion != version {
			return fmt.Errorf("Expected version %d for table %s, but got version %d in infra.table_version", version, table, readVersion)
		}
		return nil
	}
}

//ApplyOperations applies operations to a table and updates the table's version
func (r *RedshiftBackend) ApplyOperations(table string, ops []scoop_protocol.Operation, targetVersion int) error {
	lock, exist := r.tableLocks[table]
	if !exist {
		return fmt.Errorf("Lock for %s did not exist", table)
	}
	lock.Lock()
	defer lock.Unlock()

	return r.connection.ExecFnInTransaction(func(tx *sql.Tx) error {
		err := expectVersion(tx, table, targetVersion-1)
		if err != nil {
			return err
		}
		for _, op := range ops {
			switch op.Action {
			case scoop_protocol.ADD:
				mStep := migrationStep(op)
				query := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s", pq.QuoteIdentifier(table), mStep.getCreationForm())
				_, err = tx.Exec(query)
			case scoop_protocol.DELETE:
				query := fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s CASCADE", pq.QuoteIdentifier(table), pq.QuoteIdentifier(op.Name))
				_, err = tx.Exec(query)
			case scoop_protocol.RENAME:
				query := fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s",
					pq.QuoteIdentifier(table),
					pq.QuoteIdentifier(op.Name),
					pq.QuoteIdentifier(op.ActionMetadata["new_outbound"]),
				)
				_, err = tx.Exec(query)
			default:
				err = fmt.Errorf("Unknown operation action: %s", op.Action)
			}
			if err != nil {
				return err
			}
		}
		query := fmt.Sprintf("INSERT INTO infra.table_version (name, version, ts) VALUES ($1, $2, GETDATE())")
		_, err = tx.Exec(query, table, targetVersion)
		if err != nil {
			return fmt.Errorf("Error updating table_version in ace: %v", err)
		}
		return nil
	})
}

type newTable []scoop_protocol.Operation

//buildNewTable creates a newTable from a list of Operations and checks that all the operations
//are add column operations
func buildNewTable(ops []scoop_protocol.Operation) (newTable, error) {
	for _, op := range ops {
		if op.Action != scoop_protocol.ADD {
			return nil, fmt.Errorf("newTable must be made out of action=%s operations, received action=%s", scoop_protocol.ADD, op.Action)
		}
		_, cOptions := op.ActionMetadata["column_options"]
		_, cType := op.ActionMetadata["column_type"]
		if !cOptions || !cType {
			return nil, fmt.Errorf("newTable must have actionmetadata including 'column_options' and 'column_type'")
		}
	}
	return newTable(ops), nil
}

func (n *newTable) getColumnCreationString() string {
	out := bytes.NewBuffer(make([]byte, 0, 256))
	_, _ = out.WriteRune('(') // WriteRune and WriteString error always nil
	for i, op := range *n {
		step := migrationStep(op)
		_, _ = out.WriteString(step.getCreationForm())
		if i+1 != len(*n) {
			_, _ = out.WriteRune(',')
		}
	}
	_, _ = out.WriteRune(')')
	return out.String()
}

//CreateTable creates a new table at logs.`table` with the columns in ops
func (r *RedshiftBackend) CreateTable(table string, ops []scoop_protocol.Operation) error {
	newTable, err := buildNewTable(ops)
	if err != nil {
		return err
	}
	return r.connection.ExecFnInTransaction(func(tx *sql.Tx) error {
		err := expectVersion(tx, table, -1)
		if err != nil {
			return err
		}
		query := fmt.Sprintf(`CREATE TABLE %s%s;`, pq.QuoteIdentifier(table), newTable.getColumnCreationString())
		_, err = tx.Exec(query)
		if err != nil {
			return fmt.Errorf("Error CREATEing TABLE %s: %v", table, err)
		}
		r.tableLocks[table] = &sync.Mutex{}
		query = "INSERT INTO infra.table_version (name, version, ts) VALUES ($1, 0, GETDATE())"
		_, err = tx.Exec(query, table)
		if err != nil {
			return fmt.Errorf("Error updating table_version in ace: %v", err)
		}
		return nil
	})
}
