package metadata

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

func TestForceLoadExists(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.Nil(t, err, "error opening a stub database connection")
	defer func() { _ = db.Close() }()

	mock.ExpectBegin()
	mock.ExpectExec("SET TRANSACTION ISOLATION LEVEL").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("LOCK TABLE .*force_load").WillReturnResult(sqlmock.NewResult(1, 1))
	existRows := sqlmock.NewRows([]string{"exists"}).AddRow(true)
	mock.ExpectQuery("SELECT EXISTS").WithArgs("table").WillReturnRows(existRows)
	mock.ExpectCommit()

	backend := postgresBackend{db: db}
	err = backend.ForceLoad("table", "dwe")
	assert.Nil(t, err, "force load error")

	// we make sure that all expectations were met
	err = mock.ExpectationsWereMet()
	assert.Nil(t, err, "mock expectations error")
}

func TestForceLoadNotExisting(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.Nil(t, err, "error opening a stub database connection")
	defer func() { _ = db.Close() }()

	mock.ExpectBegin()
	mock.ExpectExec("SET TRANSACTION ISOLATION LEVEL").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("LOCK TABLE .*force_load").WillReturnResult(sqlmock.NewResult(1, 1))
	existRows := sqlmock.NewRows([]string{"exists"}).AddRow(false)
	mock.ExpectQuery("SELECT EXISTS").WithArgs("table").WillReturnRows(existRows)
	mock.ExpectExec("INSERT INTO force_load").WithArgs("table", "dwe").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	backend := postgresBackend{db: db}
	err = backend.ForceLoad("table", "dwe")
	assert.Nil(t, err, "force load error")

	// we make sure that all expectations were met
	err = mock.ExpectationsWereMet()
	assert.Nil(t, err, "mock expectations error")
}
