package migrator

import (
	"fmt"
	"sync"
	"time"

	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/rs_ingester/backend"
	"github.com/twitchscience/rs_ingester/blueprint"
	"github.com/twitchscience/rs_ingester/metadata"
	"github.com/twitchscience/rs_ingester/versions"
)

type tableVersion struct {
	table   string
	version int
}

// Migrator manages the migration of Ace as new versioned tsvs come in.
type Migrator struct {
	versions             versions.GetterSetter
	aceBackend           backend.Backend
	metaBackend          metadata.Reader
	bpClient             blueprint.Client
	closer               chan bool
	oldVersionWaitClose  chan bool
	wg                   sync.WaitGroup
	pollPeriod           time.Duration
	waitProcessorPeriod  time.Duration
	migrationStarted     map[tableVersion]time.Time
	offpeakStartHour     int
	offpeakDurationHours int
}

// New returns a new Migrator for migrating schemas
func New(aceBack backend.Backend,
	metaBack metadata.Reader,
	blueprintClient blueprint.Client,
	versions versions.GetterSetter,
	pollPeriod time.Duration,
	waitProcessorPeriod time.Duration,
	offpeakStartHour int,
	offpeakDurationHours int) *Migrator {
	m := Migrator{
		versions:             versions,
		aceBackend:           aceBack,
		metaBackend:          metaBack,
		bpClient:             blueprintClient,
		closer:               make(chan bool),
		oldVersionWaitClose:  make(chan bool),
		pollPeriod:           pollPeriod,
		waitProcessorPeriod:  waitProcessorPeriod,
		migrationStarted:     make(map[tableVersion]time.Time),
		offpeakStartHour:     offpeakStartHour,
		offpeakDurationHours: offpeakDurationHours,
	}

	m.wg.Add(1)
	logger.Go(func() {
		defer m.wg.Done()
		m.loop()
	})
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

//isOldVersionCleared checks to see if there are any tsvs for the given table and
//version still in queue to be loaded. If there are, it prioritizes those tsvs
//to be loaded.
func (m *Migrator) isOldVersionCleared(table string, version int) (bool, error) {
	exists, err := m.metaBackend.TSVVersionExists(table, version)
	if err != nil {
		return false, err
	}
	if !exists {
		return true, nil
	}
	return false, m.metaBackend.PrioritizeTSVVersion(table, version-1)
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
	} else {
		// to migrate, first we wait until processor finishes the old version...
		timeMigrationStarted, started := m.migrationStarted[tableVersion{table, to}]
		if !started {
			now := time.Now()
			m.migrationStarted[tableVersion{table, to}] = now
			logger.WithField("table", table).
				WithField("version", to).
				WithField("until", now.Add(m.waitProcessorPeriod)).
				Info("Starting to wait for processor before migrating")
			return nil
		}
		// don't do anything if we haven't waited long enough for processor
		if time.Since(timeMigrationStarted) < m.waitProcessorPeriod {
			logger.WithField("table", table).
				WithField("version", to).
				WithField("until", timeMigrationStarted.Add(m.waitProcessorPeriod)).
				Info("Waiting for processor before migrating")
			return nil
		}

		// wait for all the old version TSVs to ingest before proceeding
		cleared, err := m.isOldVersionCleared(table, to-1)
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

func (m *Migrator) isOffPeakHours() bool {
	currentHour := time.Now().Hour()
	if m.offpeakStartHour+m.offpeakDurationHours <= 24 {
		if (m.offpeakStartHour <= currentHour) &&
			(currentHour < m.offpeakStartHour+m.offpeakDurationHours) {
			return true
		}
		return false
	}
	// if duration bleeds into the new day, check the two segments before and after midnight
	if (m.offpeakStartHour <= currentHour) &&
		(currentHour < 24) {
		return true
	}
	if (0 <= currentHour) &&
		(currentHour < (m.offpeakStartHour+m.offpeakDurationHours)%24) {
		return true
	}
	return false
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
			if len(outdatedTables) == 0 {
				logger.Infof("Migrator didn't find any tables to migrate.")
			} else {
				logger.WithField("numTables", len(outdatedTables)).Infof("Migrator found tables to migrate.")
			}
			for _, table := range outdatedTables {
				var newVersion int
				currentVersion, exists := m.versions.Get(table)
				if !exists { // table doesn't exist yet, create it by 'migrating' to version 0
					newVersion = 0
				} else {
					newVersion = currentVersion + 1
				}
				// if not offpeak, don't migrate table, but still allow table creation
				if newVersion > 0 && !m.isOffPeakHours() {
					logger.WithField("table", table).WithField("version", newVersion).Infof("Not migrating; waiting until offpeak at %dh UTC", m.offpeakStartHour)
					continue
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
