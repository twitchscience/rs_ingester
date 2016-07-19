package redshift

import (
	"database/sql"
	"fmt"

	"github.com/lib/pq"
	"github.com/twitchscience/aws_utils/logger"
)

const schemaQuery = `
SELECT DISTINCT nspname FROM pg_namespace
WHERE nspname != 'information_schema'
AND LEFT(nspname, 3) != 'pg_'
`

//EnforcePerms is an empty struct used as the base for the TxExec method
type EnforcePerms struct{}

type tableName struct {
	Schemaname string
	Tablename  string
}

// Return true if this is an analyst mutable schema
func mutableSchema(s string) bool {
	return s == "analysis" || s == "public"
}

func grantPermsOnAllSchemas(t *sql.Tx) error {
	rows, err := t.Query(schemaQuery)
	if err != nil {
		logger.WithError(err).Error("Error fetching schemas from pg_namespace")
		return err
	}

	defer func() {
		err = rows.Close()
		if err != nil {
			logger.WithError(err).Error("Could not close rows object")
		}
	}()

	for rows.Next() {
		var s string
		if err = rows.Scan(&s); err != nil {
			logger.WithError(err).Error("Error fetching strings from row results")
			return err
		}
		err = grantPermsOnSchema(t, s)
		if err != nil {
			return err
		}
	}

	return nil
}

func grantPermsOnSchema(t *sql.Tx, s string) error {
	perms := "USAGE"
	if mutableSchema(s) {
		perms = "ALL"
	}
	_, err := t.Exec(fmt.Sprintf("GRANT %s ON SCHEMA %s TO GROUP analyst", perms, pq.QuoteIdentifier(s)))
	if err != nil {
		logger.WithError(err).WithField("schema", s).Errorf("Error setting permissions %s on schema", perms)
		return err
	}
	return nil
}

func getTables(t *sql.Tx) ([]tableName, error) {
	var tables []tableName
	rows, err := t.Query(fmt.Sprintf("SELECT schemaname, tablename FROM pg_tables WHERE schemaname IN (%s)", schemaQuery))
	if err != nil {
		logger.WithError(err).Error("Error fetching tables")
		return nil, err
	}

	defer func() {
		err = rows.Close()
		if err != nil {
			logger.WithError(err).Error("Could not close rows object")
		}
	}()

	for rows.Next() {
		t := tableName{}
		if err = rows.Scan(&t.Schemaname, &t.Tablename); err != nil {
			logger.WithError(err).Error("Error reading tables")
			return nil, err
		}
		tables = append(tables, t)
	}
	return tables, nil
}

func grantPermsOnTable(t *sql.Tx, table tableName) error {
	perms := "SELECT"
	if mutableSchema(table.Schemaname) {
		perms = "ALL"
	}

	_, err := t.Exec(fmt.Sprintf("GRANT %s ON %s.%s TO GROUP analyst",
		perms,
		pq.QuoteIdentifier(table.Schemaname),
		pq.QuoteIdentifier(table.Tablename)))

	if err != nil {
		logger.WithError(err).Error("Error setting permissions on table")
		return err
	}

	return nil
}

//TxExec uses the helper function to grant and enforce permissions on the redshift tables correctly
func (r *EnforcePerms) TxExec(t *sql.Tx) error {
	err := grantPermsOnAllSchemas(t)
	if err != nil {
		return err
	}

	tables, err := getTables(t)
	if err != nil {
		return err
	}

	for _, table := range tables {
		err = grantPermsOnTable(t, table)
		if err != nil {
			return err
		}
	}
	return nil
}
