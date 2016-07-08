package migrator

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/twitchscience/rs_ingester/backend"
	"github.com/twitchscience/rs_ingester/blueprint"
	"github.com/twitchscience/rs_ingester/metadata"
	"github.com/twitchscience/rs_ingester/scoop"
	"github.com/twitchscience/rs_ingester/versions"
)

// Migrator manages the migration of Ace as new versioned tsvs come in.
type Migrator struct {
	versions    versions.GetterSetter
	aceBackend  backend.Backend
	metaBackend metadata.Reader
	bpClient    blueprint.Client
	closer      chan bool
	wg          sync.WaitGroup
	scoopClient scoop.Client
	pollPeriod  time.Duration
}

// New returns a new Migrator for migrating schemas
func New(aceBack backend.Backend, metaBack metadata.Reader, blueprintClient blueprint.Client, scoopClient scoop.Client, versions versions.GetterSetter, pollPeriod time.Duration) *Migrator {
	m := Migrator{
		versions:    versions,
		aceBackend:  aceBack,
		metaBackend: metaBack,
		bpClient:    blueprintClient,
		scoopClient: scoopClient,
		closer:      make(chan bool),
		pollPeriod:  pollPeriod,
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
			log.Println(err)
		}
	} else {
		err = m.aceBackend.ApplyOperations(table, ops, to)
		if err != nil {
			return fmt.Errorf("Error applying operations to %s: %v", table, err)
		}
	}
	m.versions.Set(table, to)
	return nil
}

func (m *Migrator) loop() {
	log.Println("Migrator started.")
	defer log.Println("Migrator closed.")
	tick := time.NewTicker(m.pollPeriod)
	for {
		select {
		case <-tick.C:
			outdatedTables, err := m.findTablesToMigrate()
			if err != nil {
				log.Printf("Error finding migrations to apply: %v", err)
			}
			log.Printf("Migrator found %d tables to migrate.", len(outdatedTables))
			for _, table := range outdatedTables {
				var newVersion int
				currentVersion, exists := m.versions.Get(table)
				if !exists { // table doesn't exist yet, create it by 'migrating' to version 0
					newVersion = 0
				} else {
					newVersion = currentVersion + 1
				}
				log.Printf("Beginning to migrate %s to version %d", table, newVersion)
				err := m.migrate(table, newVersion)
				if err != nil {
					log.Printf("Error migrating %s to version %d: %v", table, newVersion, err)
				} else {
					log.Printf("Migrated table %s to version %d successfully.", table, newVersion)
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
