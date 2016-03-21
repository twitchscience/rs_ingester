package redshift

import (
	"fmt"

	"github.com/lib/pq"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

const (
	alterTable = `ALTER TABLE %s ADD COLUMN %s;`
)

//TableAlterRequest contains a struct that initiates alter table commands to add columns to tables
type TableAlterRequest struct {
	TableName string
	Additions []scoop_protocol.ColumnDefinition
}

//ProduceQueries creates multiple alter table queries for each column being added to a table
func (r *TableAlterRequest) ProduceQueries() []string {
	queries := make([]string, len(r.Additions))
	for idx, col := range r.Additions {
		queries[idx] = fmt.Sprintf(alterTable,
			pq.QuoteIdentifier(r.TableName),
			col.GetCreationForm())
	}
	return queries
}
