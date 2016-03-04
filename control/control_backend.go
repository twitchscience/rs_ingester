package control

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/lib/pq"
	"github.com/twitchscience/rs_ingester/constants"
)

// Event represents an event, or table, that needs to be loaded
type Event struct {
	Name      string
	Count     int
	Timestamp time.Time
}

// Backend is the backend for control, which operates on the ingester
type Backend struct {
	db *sql.DB
}

// NewControlBackend instantiates the control backend with a db connection
func NewControlBackend(db *sql.DB) *Backend {
	return &Backend{db}
}

// ForceIngest makes the given table the highest priority to load next
func (cBackend *Backend) ForceIngest(tableName string) error {
	_, err := cBackend.db.Exec(`UPDATE `+pq.QuoteIdentifier(constants.TsvTable)+` SET ts=to_timestamp(0) WHERE manifest_uuid IS NULL AND tablename=$1`, tableName)
	if err != nil {
		return fmt.Errorf("Error executing query: %v", err)
	}
	return nil
}

// GetPendingTables returns the list of tables with loads currently pending
func (cBackend *Backend) GetPendingTables() ([]Event, error) {
	var events []Event

	rows, err := cBackend.db.Query(`SELECT tablename, count(*) AS cnt, min(ts) FROM ` + pq.QuoteIdentifier(constants.TsvTable) + ` WHERE manifest_uuid IS NULL GROUP BY tablename`)
	if err != nil {
		return events, fmt.Errorf("Error executing query: %v", err)
	}
	defer func() {
		err = rows.Close()
		if err != nil {
			log.Printf("Error closing rows: %s", err)
		}
	}()
	for rows.Next() {
		var e Event
		err := rows.Scan(&e.Name, &e.Count, &e.Timestamp)
		if err != nil {
			return events, fmt.Errorf("Error reading row: %v", err)
		}
		events = append(events, e)
	}
	return events, nil
}
