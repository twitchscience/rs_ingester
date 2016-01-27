package control

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/lib/pq"
	"github.com/twitchscience/rs_ingester/constants"
)

type Event struct {
	Name      string
	Count     int
	Timestamp time.Time
}

type EventList struct {
	Events []Event
}

type ControlBackend struct {
	db *sql.DB
}

func NewControlBackend(db *sql.DB) *ControlBackend {
	return &ControlBackend{db}
}

func (cBackend *ControlBackend) ForceIngest(tableName string) error {
	_, err := cBackend.db.Exec(`UPDATE `+pq.QuoteIdentifier(constants.TsvTable)+` SET ts=to_timestamp(0) WHERE manifest_uuid IS NULL AND tablename=$1`, tableName)
	if err != nil {
		return fmt.Errorf("Error executing query: %v", err)
	}
	return nil
}

func (cBackend *ControlBackend) GetPendingTables() (EventList, error) {
	eventList := EventList{[]Event{}}

	rows, err := cBackend.db.Query(`SELECT tablename, count(*) AS cnt, min(ts) FROM ` + pq.QuoteIdentifier(constants.TsvTable) + ` WHERE manifest_uuid IS NULL GROUP BY tablename`)
	if err != nil {
		return eventList, fmt.Errorf("Error executing query: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var event Event
		err := rows.Scan(&event.Name, &event.Count, &event.Timestamp)
		if err != nil {
			return eventList, fmt.Errorf("Error reading row: %v", err)
		}
		eventList.Events = append(eventList.Events, event)
	}
	return eventList, nil
}
