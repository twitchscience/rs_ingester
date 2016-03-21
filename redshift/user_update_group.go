package redshift

import (
	"database/sql"
	"fmt"
	"log"
)

const (
	groupAddCommand = `ALTER GROUP %s ADD USER %s`
)

//UpdateGroup is a struct that that holds information to update the group a user is part of
type UpdateGroup struct {
	User  string
	Group string
}

//TxExec executes the query that alters a group to include a new user
func (r *UpdateGroup) TxExec(t *sql.Tx) error {
	_, err := t.Exec(fmt.Sprintf(groupAddCommand, r.Group, r.User))
	if err != nil {
		log.Printf("Error on adding user to group: %v", err)
		return err
	}
	return nil
}
