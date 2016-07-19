package redshift

import (
	"database/sql"
	"fmt"

	"github.com/twitchscience/aws_utils/logger"
)

const (
	superuserCommand = `ALTER USER %s CREATEUSER`
)

//MakeSuperuser is a struct that contains the user that needs to become super user
type MakeSuperuser struct {
	User string
}

//TxExec executes the query that makes a user a superuser
func (r *MakeSuperuser) TxExec(t *sql.Tx) error {
	_, err := t.Exec(fmt.Sprintf(superuserCommand, r.User))
	if err != nil {
		logger.WithError(err).Error("Error on making superuser")
		return err
	}
	return nil
}
