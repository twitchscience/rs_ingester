package redshift

import (
	"database/sql"
	"fmt"

	"github.com/twitchscience/aws_utils/logger"
)

const (
	creationCommand = `CREATE USER %s IN GROUP %s PASSWORD '%s'`
)

//NewUser is a struct that represents a user
type NewUser struct {
	User     string
	Password string
}

//TxExec executes the query for creating a user in redshift
func (r *NewUser) TxExec(t *sql.Tx) error {
	_, err := t.Exec(fmt.Sprintf(creationCommand, r.User, "analyst", r.Password))
	if err != nil {
		logger.WithError(err).Error("Error on creating user")
		return err
	}
	return nil
}
