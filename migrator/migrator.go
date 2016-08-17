package migrator

import (
	"fmt"
	"sync"
	"time"

	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/rs_ingester/backend"
	"github.com/twitchscience/rs_ingester/blueprint"
	"github.com/twitchscience/rs_ingester/metadata"
	"github.com/twitchscience/rs_ingester/scoop"
	"github.com/twitchscience/rs_ingester/versions"
)

type tableVersion struct {
	table   string
	version int
}

// Migrator manages the migration of Ace as new versioned tsvs come in.
type Migrator struct {
	versions            versions.GetterSetter
	aceBackend          backend.Backend
	metaBackend         metadata.Reader
	bpClient            blueprint.Client
	closer              chan bool
	oldVersionWaitClose chan bool
	wg                  sync.WaitGroup
	scoopClient         scoop.Client
	pollPeriod          time.Duration
	waitProcessorPeriod time.Duration
	migrationStarted    map[tableVersion]time.Time
}

// New returns a new Migrator for migrating schemas
func New(aceBack backend.Backend, metaBack metadata.Reader, blueprintClient blueprint.Client, scoopClient scoop.Client, versions versions.GetterSetter, pollPeriod time.Duration, waitProcessorPeriod time.Duration) *Migrator {
	m := Migrator{
		versions:            versions,
		aceBackend:          aceBack,
		metaBackend:         metaBack,
		bpClient:            blueprintClient,
		scoopClient:         scoopClient,
		closer:              make(chan bool),
		oldVersionWaitClose: make(chan bool),
		pollPeriod:          pollPeriod,
		waitProcessorPeriod: waitProcessorPeriod,
		migrationStarted:    make(map[tableVersion]time.Time),
	}

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.loop()
	}()
	return &m
}

// findTablesToMigrate inspects tsvs waiting to be loaded and compares their versions
// with the current versions, returning table names to be migrated up
func (m *Migrator) findTablesToMigrate() ([]string, error) {
	tsvVersions, err := m.metaBackend.Versions()
	if err != nil {
		return nil, fmt.Errorf("Error finding versions from unloaded tsvs: %v", err)
	}
	var tables []string
	for tsvTable, tsvVersion := range tsvVersions {
		aceVersion, existant := m.versions.Get(tsvTable)
		if !existant || tsvVersion > aceVersion {
			tables = append(tables, tsvTable)
		}
	}
	return tables, nil
}

func (m *Migrator) isOldVersionCleared(table string, to int) (bool, error) {
	exists, err := m.metaBackend.TSVVersionExists(table, to-1)
	if err != nil {
		return false, err
	}
	if !exists {
		return true, nil
	}
	return false, m.metaBackend.PrioritizeTSVVersion(table, to-1)
}

func (m *Migrator) migrate(table string, to int) error {
	ops, err := m.bpClient.GetMigration(table, to)
	if err != nil {
		return err
	}
	if to == 0 {
		err = m.aceBackend.CreateTable(table, ops)
		if err != nil {
			return err
		}
		err = m.scoopClient.EnforcePermissions()
		if err != nil {
			logger.WithError(err).Error("Problem enforcing permissions through scoop")
		}
	} else {
		// to migrate, first we wait until processor finishes the old version...
		timeMigrationStarted, started := m.migrationStarted[tableVersion{table, to}]
		if !started {
			m.migrationStarted[tableVersion{table, to}] = time.Now()
			logger.WithField("table", table).WithField("version", to).Info("Starting to wait for processor before migrating")
			return nil
		}
		// don't do anything if we haven't waited long enough for processor
		if time.Since(timeMigrationStarted) < m.waitProcessorPeriod {
			logger.WithField("table", table).WithField("version", to).Info("Waiting for processor before migrating")
			return nil
		}

		// wait for all the old version TSVs to ingest before proceeding
		cleared, err := m.isOldVersionCleared(table, to)
		if err != nil {
			return fmt.Errorf("Error waiting for old version to clear: %v", err)
		}
		if !cleared {
			logger.WithField("table", table).WithField("version", to).Info("Waiting for old version to clear.")
			return nil
		}

		// everything is ready, now actually do the migration
		logger.WithField("table", table).WithField("version", to).Info("Beginning to migrate")
		err = m.aceBackend.ApplyOperations(table, ops, to)
		if err != nil {
			return fmt.Errorf("Error applying operations to %s: %v", table, err)
		}
	}
	m.versions.Set(table, to)
	logger.WithField("table", table).WithField("version", to).Info("Migrated table successfully")

	return nil
}

func (m *Migrator) loop() {
	logger.Info("Migrator started.")
	defer logger.Info("Migrator stopped.")
	tick := time.NewTicker(m.pollPeriod)
	for {
		select {
		case <-tick.C:
			outdatedTables, err := m.findTablesToMigrate()
			if err != nil {
				logger.WithError(err).Error("Error finding migrations to apply")
			}
			logger.WithField("numTables", len(outdatedTables)).Infof("Migrator found tables to migrate.")
			for _, table := range outdatedTables {
				var newVersion int
				currentVersion, exists := m.versions.Get(table)
				if !exists { // table doesn't exist yet, create it by 'migrating' to version 0
					newVersion = 0
				} else {
					newVersion = currentVersion + 1
				}
				err := m.migrate(table, newVersion)
				if err != nil {
					logger.WithError(err).WithField("table", table).WithField("version", newVersion).Error("Error migrating table")
				}
			}
		case <-m.closer:
			return
		}
	}
}

// Close signals the migrator to stop looking for new migrations and waits until
// it's finished any migrations.
func (m *Migrator) Close() {
	m.closer <- true
	m.wg.Wait()
}
