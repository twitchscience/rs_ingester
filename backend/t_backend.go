package backend

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"log"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/twitchscience/rs_ingester/redshift"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

var cfgs = []scoop_protocol.Config{
	scoop_protocol.Config{
		EventName: "minute-watched",
		Columns: []scoop_protocol.ColumnDefinition{
			scoop_protocol.ColumnDefinition{
				InboundName:           "time",
				OutboundName:          "timestamp",
				Transformer:           "f@timestamp@unix",
				ColumnCreationOptions: "sortkey",
			},
			scoop_protocol.ColumnDefinition{
				InboundName:           "player",
				OutboundName:          "player",
				Transformer:           "varchar",
				ColumnCreationOptions: "(16)",
			},
		},
	},
	scoop_protocol.Config{
		EventName: "buffer-empty",
		Columns: []scoop_protocol.ColumnDefinition{
			scoop_protocol.ColumnDefinition{
				InboundName:           "time",
				OutboundName:          "when",
				Transformer:           "f@timestamp@unix",
				ColumnCreationOptions: "sortkey",
			},
			scoop_protocol.ColumnDefinition{
				InboundName:           "ip",
				OutboundName:          "city",
				Transformer:           "ipCity",
				ColumnCreationOptions: "",
			},
			scoop_protocol.ColumnDefinition{
				InboundName:           "ip",
				OutboundName:          "country",
				Transformer:           "ipCountry",
				ColumnCreationOptions: "",
			},
			scoop_protocol.ColumnDefinition{
				InboundName:           "ip",
				OutboundName:          "region",
				Transformer:           "ipRegion",
				ColumnCreationOptions: "",
			},
		},
	},
}

//TestBackend holds a simulated connection to redshift
type TestBackend struct {
	simulatedBackend RedshiftBackend
}

//BuildTestBackend makes a TestBackend
func BuildTestBackend() *TestBackend {
	db, err := sqlmock.New()
	if err != nil {
		log.Fatalf("An error '%s' was not expected when opening a stub database connection", err)
	}

	conn := &redshift.RSConnection{
		Conn:            db,
		InboundRequests: make(chan redshift.RSRequest, 10),
	}

	session := session.New()

	r := RedshiftBackend{
		connection:  conn,
		credentials: session.Config.Credentials,
	}
	return &TestBackend{
		simulatedBackend: r,
	}
}

//HealthCheck retursn nil all the time
func (t *TestBackend) HealthCheck() error {
	return nil
}

//Create Creates a new table on the simulated test Backend
func (t *TestBackend) Create(newTable *scoop_protocol.Config) error {
	for _, cfg := range cfgs {
		if cfg.EventName == newTable.EventName {
			return errors.New("Table already exists")
		}
	}
	cfgs = append(cfgs, *newTable)
	return nil
}

//Update updates a table on the simulated test backend
func (t *TestBackend) Update(additions *scoop_protocol.Config) error {
	for idx, cfg := range cfgs {
		if cfg.EventName == additions.EventName {
			for _, add := range additions.Columns {
				for _, old := range cfg.Columns {
					if old.OutboundName == add.OutboundName {
						return fmt.Errorf("Column %s already exists", add.OutboundName)
					}
				}
			}

			cfgs[idx].Columns = append(cfgs[idx].Columns, additions.Columns...)
		}
	}
	return nil
}

//Query swallows a query and basically does nothing
func (t *TestBackend) Query(req *redshift.QueryRequest) ([]byte, error) {
	log.Printf("Query request: %v\n", req)
	time.Sleep(60 * time.Second)
	return []byte("hahahahahhaha"), nil
}

//AnyArg is a helper empty struct for Copy
type AnyArg struct{}

//Match returns true for any driver value
func (a AnyArg) Match(d driver.Value) bool {
	return true
}

//Copy copies a row to the simulated redshift backend
func (t *TestBackend) Copy(rc *scoop_protocol.RowCopyRequest) error {
	anyArg := AnyArg{}
	log.Printf("Copy request: %v\n", rc)

	sqlmock.ExpectBegin()
	sqlmock.ExpectExec(fmt.Sprintf(`COPY "%s" FROM \$1 .* \$2 .*`, rc.TableName)).WithArgs(
		"s3://"+rc.KeyName, anyArg).WillReturnResult(sqlmock.NewResult(0, 1))
	sqlmock.ExpectCommit()

	err := t.simulatedBackend.Copy(rc)
	if err != nil {
		log.Printf("Got error during Copy: %s", err.Error())
		return err
	}

	err = t.simulatedBackend.connection.Conn.Close()
	if err != nil {
		log.Printf("Got error closing fake connection: %s", err.Error())
		return err
	}

	db, dbErr := sqlmock.New()
	if dbErr != nil {
		log.Fatalf("An error '%s' was not expected when opening a stub database connection", err)
	}

	t.simulatedBackend.connection.Conn = db

	log.Print("Copy executed all expected queries")
	return err
}

//ManifestCopy returns nil
func (t *TestBackend) ManifestCopy(rc *scoop_protocol.ManifestRowCopyRequest) error {
	return nil
}

//LoadCheck returns scoop_protocol.LoadComplete
func (t *TestBackend) LoadCheck(rs *scoop_protocol.LoadCheckRequest) (resp *scoop_protocol.LoadCheckResponse, err error) {
	resp.LoadStatus = scoop_protocol.LoadComplete
	return
}

//AllSchemas returns the declared cfgs in the file
func (t *TestBackend) AllSchemas() ([]scoop_protocol.Config, error) {
	return cfgs, nil
}

//Schema returns the first schema from cfgs
func (t *TestBackend) Schema(event string) (*scoop_protocol.Config, error) {
	s, err := t.AllSchemas()
	if err != nil {
		return nil, err
	}
	return &s[0], nil
}

//EnforcePermissions returns nil
func (t *TestBackend) EnforcePermissions() error {
	return nil
}

//NewUser returns nil
func (t *TestBackend) NewUser(user, pw string) error {
	return nil
}

//MakeSuperuser returns nil
func (t *TestBackend) MakeSuperuser(user string) error {
	return nil
}

//UpdateGroup returns nil
func (t *TestBackend) UpdateGroup(user, group string) error {
	return nil
}

//UpdatePassword returns nil
func (t *TestBackend) UpdatePassword(user, pw string) error {
	return nil
}
