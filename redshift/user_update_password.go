package redshift

import (
	"database/sql"
	"fmt"

	"github.com/twitchscience/aws_utils/logger"
)

const (
	userPwCommand = `ALTER USER %s PASSWORD '%s'`
)

//UpdatePassword is a struct that holds the information necessary to change a user's password
type UpdatePassword struct {
	User     string
	Password string
}

//TxExec executes the query that changes a user's password
func (r *UpdatePassword) TxExec(t *sql.Tx) error {
	_, err := t.Exec(fmt.Sprintf(userPwCommand, r.User, r.Password))
	if err != nil {
		logger.WithError(err).Error("Error on altering password")
		return err
	}
	return nil
}
