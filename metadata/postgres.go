package metadata

/* Postgres-based backend */

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"time"

	_ "github.com/lib/pq" // To register "postgres" with database/sql
	"github.com/pborman/uuid"
	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/rs_ingester/versions"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

// PGConfig stores configuration for postgres
type PGConfig struct {
	DatabaseURL      string
	LoadAgeTrigger   time.Duration
	LoadCountTrigger int
	MaxConnections   int
}

type loadChecker interface {
	CheckLoad(manifestUUID string) (scoop_protocol.LoadStatus, error)
}

type loadableTable struct {
	name        string
	version     int
	forceLoadID *int
}

type postgresBackend struct {
	db            *sql.DB
	cfg           *PGConfig
	loadChecker   loadChecker
	wait          chan struct{}
	loadReady     chan *LoadManifest
	gracefulClose chan struct{}
	versions      versions.Getter
}

var (
	errorNoTsvs             = errors.New("No tsvs were found with that manifest id")
	errorNoLoads            = errors.New("Found no loads to do")
	tableToLoadSearchSize   = 50
	maxLoadRetryCount       int
	dbRetryCount            int
	noWorkDelay             time.Duration
	errorRetryDelay         time.Duration
	failedLoadCheckInterval time.Duration
)

func init() {
	flag.DurationVar(&noWorkDelay, "no_work_delay", time.Minute, "Time to wait if there's no work to do")
	flag.IntVar(&maxLoadRetryCount, "max_load_retry", 5, "Number of times to retry a load manifest before giving up")
	flag.IntVar(&dbRetryCount, "max_db_retry", 10, "Number of times to retry a transaction")
	flag.DurationVar(&errorRetryDelay, "error_retry_delay", time.Minute*15, "Time to wait to retry a load that errors")
	flag.DurationVar(&failedLoadCheckInterval, "failed_load_check_interval", time.Minute, "How often to check for failed loads")
}

// NewPostgresReader configures a new postgres backend for reading only
func NewPostgresReader(cfg *PGConfig, versions versions.Getter) (Reader, error) {
	b := &postgresBackend{
		cfg:       cfg,
		loadReady: nil,
		wait:      make(chan struct{}),
		versions:  versions,
	}

	err := b.connectBackendToDB()
	if err != nil {
		return nil, err
	}

	return b, nil
}

// NewPostgresStorer configures a new postgres backend for storing only
func NewPostgresStorer(cfg *PGConfig) (Storer, error) {
	b := &postgresBackend{
		cfg:       cfg,
		loadReady: nil,
		wait:      make(chan struct{}),
	}

	err := b.connectBackendToDB()
	if err != nil {
		return nil, err
	}

	return b, nil
}

// NewPostgresLoader configures a new postgres backend for loading (or storing)
// At backend configuration, we set a max number of tsvs for a table
// and max count of tsvs before a load is triggered.
func NewPostgresLoader(cfg *PGConfig, lChecker loadChecker, versions versions.Getter) (Backend, error) {
	b := &postgresBackend{
		cfg:           cfg,
		loadChecker:   lChecker,
		loadReady:     make(chan *LoadManifest),
		wait:          make(chan struct{}),
		gracefulClose: make(chan struct{}),
		versions:      versions,
	}

	err := b.connectBackendToDB()
	if err != nil {
		return nil, err
	}

	err = b.checkOrphanedLoads()
	if err != nil {
		return nil, fmt.Errorf("Failed to check orphaned loads: %s", err)
	}

	logger.Go(b.loadReadyWorker)

	return b, nil
}

func (b *postgresBackend) checkOrphanedLoads() error {
	rows, err := b.db.Query(`SELECT uuid FROM manifest WHERE retry_ts IS NULL`)
	if err != nil {
		return err
	}

	defer func() {
		err = rows.Close()
		if err != nil {
			logger.WithError(err).Error("Error closing rows for orphan load check")
		}
	}()

	orphanUUIDs := []string{}
	for rows.Next() {
		var uuid string
		err = rows.Scan(&uuid)
		if err != nil {
			return fmt.Errorf("Got error fetching orphaned loads: %v", err)
		}
		orphanUUIDs = append(orphanUUIDs, uuid)
	}

	for _, orphanUUID := range orphanUUIDs {
		loadStatus, err := b.loadChecker.CheckLoad(orphanUUID)
		if err != nil {
			logger.WithError(err).Error("Got error checking orphan load status")
			return err
		}

		tx, err := b.db.Begin()
		if err != nil {
			logger.WithError(err).Error("Error beginning transaction")
			return err
		}

		if err = isolateTransaction(tx); err != nil {
			logger.WithError(err).Error("Error setting transaction serializable")
			return rollbackAndError(tx, err)
		}

		switch loadStatus {
		case scoop_protocol.LoadComplete:
			// If completed succesfully, delete tsv rows
			logger.WithField("orphanUUID", orphanUUID).Info("Orphaned load is complete, marking done")
			err = b.loadDoneHelper(tx, orphanUUID) // loadDoneHelper rolls back on error
			if err != nil {
				return err
			}

			err = tx.Commit()
			if err != nil {
				logger.WithError(err).Error("Error on commit when cleaning up manifests+tsvs at finished orphan load")
				return err
			}

		case scoop_protocol.LoadNotFound, scoop_protocol.LoadFailed:
			// If completed succesfully, delete tsv rows
			logger.WithField("orphanUUID", orphanUUID).Info("Orphaned load failed, marking for retry")
			err = b.loadErrorHelper(tx, orphanUUID, "Orphan load on startup") // loadErrorHelper rolls back on error
			if err != nil {
				return err
			}

			err = tx.Commit()
			if err != nil {
				logger.WithError(err).Error("Error on commit when marking failed orphan load for retrial")
				return err
			}
		default:
			err = tx.Commit()
			if err != nil {
				logger.WithError(err).Error("Got unexpected load status from orphan load check")
				return err
			}
		}
	}

	return nil
}

func (b *postgresBackend) InsertLoad(load *Load) error {
	_, err := b.db.Exec(
		"INSERT INTO tsv (tablename, keyname, tableversion, ts) VALUES ($1, $2, $3, $4)",
		load.TableName,
		load.KeyName,
		load.TableVersion,
		time.Now().In(time.UTC),
	)
	return err
}

func (b *postgresBackend) LoadReady() chan *LoadManifest {
	return b.loadReady
}

func (b *postgresBackend) LoadDone(manifestUUID string) {
	err := retryInTransaction(dbRetryCount, b.db, func(tx *sql.Tx) error {
		return b.loadDoneHelper(tx, manifestUUID)
	})
	if err != nil {
		logger.WithError(err).WithField("manifestUUID", manifestUUID).
			Error("Error marking load as done and used all retries; final error attached")
	}
}

func (b *postgresBackend) LoadError(manifestUUID string, loadError string) {
	err := retryInTransaction(dbRetryCount, b.db, func(tx *sql.Tx) error {
		return b.loadErrorHelper(tx, manifestUUID, loadError)
	})
	if err != nil {
		logger.WithError(err).WithField("manifestUUID", manifestUUID).
			Error("Error marking load as error and used all retries; final error attached")
	}
}

func (b *postgresBackend) Versions() (map[string]int, error) {
	rows, err := b.db.Query(`SELECT tablename, MAX(tableversion) FROM tsv GROUP BY tablename;`)
	if err != nil {
		return nil, err
	}

	defer func() {
		err = rows.Close()
		if err != nil {
			logger.WithError(err).Error("Error closing rows for versions")
		}
	}()

	ret := make(map[string]int)
	for rows.Next() {
		var table string
		var version int
		err = rows.Scan(&table, &version)
		ret[table] = version

		if err != nil {
			return nil, fmt.Errorf("Got error fetching unloaded tsv versions: %v", err)
		}
	}
	return ret, nil
}

// Close the backend; signals the loadready worker to end gracefully if it is running
func (b *postgresBackend) Close() {
	close(b.wait)
	if b.gracefulClose != nil {
		<-b.gracefulClose
	}
}

// Non-commiting load done helper function
func (b *postgresBackend) loadDoneHelper(tx *sql.Tx, manifestUUID string) error {
	_, err := tx.Exec("DELETE FROM tsv WHERE manifest_uuid = $1", manifestUUID)
	if err != nil {
		return rollbackAndError(tx, err)
	}

	_, err = tx.Exec("DELETE FROM manifest WHERE uuid = $1", manifestUUID)
	if err != nil {
		return rollbackAndError(tx, err)
	}

	return nil
}

func (b *postgresBackend) loadErrorHelper(tx *sql.Tx, manifestUUID, loadError string) error {
	_, err := tx.Exec("UPDATE manifest SET retry_ts = $1, last_error = $2 WHERE uuid = $3",
		time.Now().In(time.UTC).Add(errorRetryDelay),
		loadError,
		manifestUUID)
	if err != nil {
		return rollbackAndError(tx, err)
	}

	return nil
}

func (b *postgresBackend) loadReadyWorker() {
	logger.Info("Starting loadReadyWorker.")
	defer logger.Info("loadReadyWorker stopped.")

	var lastFailedLoadCheck time.Time
	for {
		var failed *LoadManifest

		if time.Now().In(time.UTC).Sub(lastFailedLoadCheck) > failedLoadCheckInterval {
			err := retrying(dbRetryCount, func() error {
				var err error
				failed, err = b.fetchFailedLoad()
				return err
			})
			if err == nil {
				lastFailedLoadCheck = time.Now().In(time.UTC)
			} else {
				logger.WithError(err).Error("Error checking failed loads")
			}

			if failed != nil {
				b.loadReady <- failed
			}
		}

		var manifest *LoadManifest

		err := retrying(dbRetryCount, func() error {
			var err error
			manifest, err = b.fetchLoad()
			return err
		})
		if err != nil {
			logger.WithError(err).Errorf("Error fetching manifest for load after %d tries. Last error attached", dbRetryCount)
		}

		sleepDelay := noWorkDelay
		if manifest != nil {
			b.loadReady <- manifest
			sleepDelay = time.Millisecond * 10
		}

		select {
		case <-time.After(sleepDelay):
		case <-b.wait:
			close(b.loadReady)
			close(b.gracefulClose)
			return
		}
	}
}

// Check for failed loads and retry them up if applicable
func (b *postgresBackend) fetchFailedLoad() (*LoadManifest, error) {
	tx, err := b.db.Begin()
	if err != nil {
		logger.WithError(err).Error("Error beginning transaction")
		return nil, err
	}

	if err = isolateTransaction(tx); err != nil {
		logger.WithError(err).Error("Error setting transaction serializable")
		return nil, rollbackAndError(tx, err)
	}

	loadUUID, lastError, err := failedLoadMetadata(tx)
	if err != nil {
		return nil, rollbackAndError(tx, err)
	}
	err = tx.Commit()
	if err != nil {
		logger.WithError(err).Error("Error on commit when locking manifest load")
		return nil, err
	}
	if loadUUID == "" {
		return nil, nil
	}

	logger.WithField("loadUUID", loadUUID).Info("Fetching failed load")

	tx, err = b.db.Begin()
	if err != nil {
		logger.WithError(err).Error("Error beginning transaction")
		return nil, err
	}

	if err = isolateTransaction(tx); err != nil {
		logger.WithError(err).Error("Error setting transaction serializable")
		return nil, rollbackAndError(tx, err)
	}

	logger.WithError(err).WithField("loadUUID", loadUUID).
		WithField("error", lastError.String).
		Infof("Load failed and has a known error, retrying manifest")
	var tsv *LoadManifest
	tsv, err = getLoadManifest(tx, loadUUID)
	if err != nil {
		return nil, rollbackAndError(tx, err)
	}

	return tsv, tx.Commit()
}

func failedLoadMetadata(tx *sql.Tx) (loadUUID string, lastError sql.NullString, err error) {
	now := time.Now().In(time.UTC)
	rows, err := tx.Query(`UPDATE manifest
                               SET retry_ts = null,
							       retry_count = retry_count + 1
                               WHERE uuid IN (
                                 SELECT uuid
                                 FROM manifest
                                 WHERE retry_ts IS NOT NULL AND retry_ts < $1 AND retry_count < $2
                                 ORDER BY retry_ts ASC
                                 LIMIT 1
                               )
                               RETURNING uuid, last_error
                              `, now, maxLoadRetryCount)

	if err != nil {
		logger.WithError(err).Error("Error querying for failed loads")
		return
	}
	defer func() {
		err = rows.Close()
		if err != nil {
			logger.WithError(err).Error("Error closing rows for failed load metadata")
		}
	}()

	if rows.Next() {
		err = rows.Scan(&loadUUID, &lastError)
		if err != nil {
			logger.WithError(err).Error("Got error fetching tsv row")
			return
		}
	}
	return
}

// ConnectToDB opens a postgres connection to the db with max open connections
func ConnectToDB(dbURL string, maxConnections int) (*sql.DB, error) {
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		return nil, fmt.Errorf("Got err %v while connecting to rds", err)
	}
	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("Could not ping rds %v", err)
	}
	db.SetMaxOpenConns(maxConnections)
	return db, nil
}

func (b *postgresBackend) connectBackendToDB() error {
	db, err := ConnectToDB(b.cfg.DatabaseURL, b.cfg.MaxConnections)
	b.db = db
	return err
}

func (b *postgresBackend) PingDB() error {
	err := b.db.Ping()
	if err != nil {
		return fmt.Errorf("Could not ping rds %v", err)
	}
	return nil
}

func (b *postgresBackend) findTableVersionToLoad(tx *sql.Tx) (*loadableTable, error) {
	rows, err := tx.Query(`
		SELECT tablename, tableversion, force_load_id FROM
			(SELECT tsv.tablename,
				tableversion,
				min(tsv.ts) AS oldest,
				unstarted_force_load.id AS force_load_id,
				count(*) AS cnt
			FROM tsv LEFT JOIN (
				SELECT id, tablename
				FROM force_load
				WHERE force_load.started IS NULL
			) AS unstarted_force_load
			ON tsv.tablename=unstarted_force_load.tablename
			WHERE manifest_uuid IS NULL
			GROUP BY tsv.tablename, tableversion, force_load_id) a
		WHERE (cnt > $1 OR oldest < $2 OR force_load_id IS NOT NULL)
		ORDER BY force_load_id ASC, oldest ASC
		LIMIT $3`,
		b.cfg.LoadCountTrigger,
		time.Now().In(time.UTC).Add(-b.cfg.LoadAgeTrigger),
		tableToLoadSearchSize,
	)
	if err != nil {
		return nil, fmt.Errorf("Error finding potential tables to load: %v", err)
	}

	defer func() {
		err = rows.Close()
		if err != nil {
			logger.WithError(err).Error("Error closing rows")
		}
	}()

	var tableToLoad loadableTable
	found := false
	for rows.Next() && !found {
		if err = rows.Scan(&tableToLoad.name, &tableToLoad.version, &tableToLoad.forceLoadID); err != nil {
			return nil, fmt.Errorf("Error parsing rows when looking for potential tables to load: %v", err)
		}
		currentVersion, exists := b.versions.Get(tableToLoad.name)
		if exists && tableToLoad.version < currentVersion {
			logger.WithField("table", tableToLoad.name).
				WithField("outdatedVersion", tableToLoad.version).
				WithField("currentVersion", currentVersion).
				Error("Found a TSV with an outdated version")
		}
		if exists && tableToLoad.version == currentVersion {
			found = true
		}
	}
	if !found {
		logger.Info("Found no loads to do")
		return nil, errorNoLoads
	}
	return &tableToLoad, nil
}

// fetchLoad returns the next load to do, or nil if there is no load available.
func (b *postgresBackend) fetchLoad() (*LoadManifest, error) {
	tx, err := b.db.Begin()
	if err != nil {
		return nil, err
	}

	if err = isolateTransaction(tx); err != nil {
		logger.WithError(err).Error("Error on setting serializable")
		return nil, rollbackAndError(tx, err)
	}

	manifestUUID := uuid.NewRandom().String()

	_, err = tx.Exec(`INSERT INTO manifest (uuid)
                      VALUES ($1)
                     `, manifestUUID)
	if err != nil {
		logger.WithError(err).Error("Error inserting into manifest")
		return nil, rollbackAndError(tx, err)
	}

	tableToLoad, err := b.findTableVersionToLoad(tx)
	if err != nil {
		if err == errorNoLoads {
			return nil, tx.Rollback()
		}
		return nil, rollbackAndError(tx, err)
	}

	_, err = tx.Exec(
		`UPDATE tsv SET manifest_uuid = $1
         WHERE tablename = $2
         AND tableversion = $3
         AND manifest_uuid IS NULL
        `,
		manifestUUID,
		tableToLoad.name,
		tableToLoad.version,
	)

	if err != nil {
		return nil, rollbackAndError(tx, err)
	}

	if tableToLoad.forceLoadID != nil {
		_, err = tx.Exec(
			`UPDATE force_load SET started = NOW()
			 WHERE id = $1 AND started IS NULL
		`,
			tableToLoad.forceLoadID,
		)

		if err != nil {
			return nil, rollbackAndError(tx, err)
		}
	}

	tsv, err := getLoadManifest(tx, manifestUUID)
	if err != nil {
		if err == errorNoTsvs {
			return nil, tx.Rollback()
		}
		return nil, rollbackAndError(tx, err)
	}

	return tsv, tx.Commit()
}

func getLoadManifest(tx *sql.Tx, manifestUUID string) (*LoadManifest, error) {
	var manifest LoadManifest
	manifest.UUID = manifestUUID

	rows, err := tx.Query("SELECT keyname, tablename FROM tsv WHERE manifest_uuid = $1", manifestUUID)
	if err != nil {
		return nil, err
	}

	defer func() {
		err = rows.Close()
		if err != nil {
			logger.WithError(err).Error("Error closing rows for load manifest")
		}
	}()
	for rows.Next() {
		var load Load
		err := rows.Scan(&load.KeyName, &load.TableName)
		if err != nil {
			logger.WithError(err).Error("Scan threw an error")
			return nil, err
		}

		manifest.Loads = append(manifest.Loads, load)
	}

	if len(manifest.Loads) == 0 {
		return nil, errorNoTsvs
	}

	manifest.TableName = manifest.Loads[0].TableName

	return &manifest, nil
}

func retrying(retryCount int, f func() error) (err error) {
	for ; retryCount > 0; retryCount-- {
		err = f()
		if err != nil {
			time.Sleep(time.Duration(rand.Intn(1000)+500) * time.Millisecond)
		} else {
			return
		}
	}
	return
}

func retryInTransaction(retryCount int, db *sql.DB, f func(tx *sql.Tx) error) error {
	return retrying(retryCount, func() error {
		tx, err := db.Begin()
		if err != nil {
			logger.WithError(err).Error("Error beginning transaction")
			return err
		}

		if err = isolateTransaction(tx); err != nil {
			logger.WithError(err).Error("Error setting transaction serializable")
			return err
		}

		err = f(tx)

		if err != nil {
			logger.WithError(err).Error("Error in transaction")
			return rollbackAndError(tx, err)
		}

		return tx.Commit()
	})
}

func isolateTransaction(tx *sql.Tx) error {
	_, err := tx.Exec("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
	if err != nil {
		return err
	}
	_, err = tx.Exec("LOCK TABLE tsv, manifest, force_load")
	return err
}

func rollbackAndError(tx *sql.Tx, err error) error {
	newErr := tx.Rollback()
	if newErr != nil {
		return fmt.Errorf("Rollback error (%v); previous error (%v)", newErr, err)
	}
	return err
}

func (b *postgresBackend) TSVVersionExists(table string, version int) (bool, error) {
	row := b.db.QueryRow("SELECT exists(SELECT 1 FROM tsv WHERE tablename = $1 AND tableversion = $2)",
		table,
		version)
	var exists bool
	err := row.Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("Got error fetching amount of TSVs in a version: %v", err)
	}
	return exists, nil
}

func (b *postgresBackend) ForceLoad(table string, requester string) error {
	err := retryInTransaction(1, b.db, func(tx *sql.Tx) error {
		row := tx.QueryRow(
			"SELECT EXISTS(SELECT 1 FROM force_load WHERE tablename = $1 AND started IS NULL)",
			table)
		var exists bool
		err := row.Scan(&exists)
		if err != nil {
			return fmt.Errorf("Got error checking for existing force load: %v", err)
		}
		if exists {
			return nil
		}
		_, err = tx.Exec(
			"INSERT INTO force_load (tablename, requester, ts) VALUES ($1, $2, NOW())",
			table, requester,
		)
		return err
	})
	if err != nil {
		return fmt.Errorf("forcing load: %v", err)
	}
	return err
}

func findOrCreateStat(loadStats *PendingLoadStats, event string) *EventStats {
	for _, s := range loadStats.Stats {
		if s.Event == event {
			return s
		}
	}
	eventStats := &EventStats{Event: event}
	loadStats.Stats = append(loadStats.Stats, eventStats)
	return eventStats
}

func updateStats(loadStats *PendingLoadStats, event string, cnt int64, minTS time.Time) {
	if cnt == 0 {
		return
	}
	eventStats := findOrCreateStat(loadStats, event)
	eventStats.Count += cnt
	if eventStats.MinTS.IsZero() || eventStats.MinTS.After(minTS) {
		eventStats.MinTS = minTS
	}
}

// StatsForPendingLoads returns aggregates stats for each type of pending load classification.
func (b *postgresBackend) StatsForPendingLoads() ([]*PendingLoadStats, error) {
	rows, err := b.db.Query(`
		SELECT
			tablename,
			tableversion,
			is_stale,
			min(ts),
			count(*)
		FROM (
			SELECT
				tablename,
				tableversion,
				retry_count IS NOT NULL AND retry_count >= $1 AS is_stale,
				ts
			FROM tsv
			LEFT JOIN manifest
				ON manifest_uuid=uuid
		) tbl
		GROUP BY 1, 2, 3
	`, maxLoadRetryCount)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %v", err)
	}
	defer func() {
		err = rows.Close()
		if err != nil {
			logger.WithError(err).Error("error closing rows")
		}
	}()

	// Since we don't store the current event versions in the db, we'll take the returned groups
	// and identify which ones are pending migration in the logic below
	var table string
	var version int64
	var isStale bool
	var minTS time.Time
	var cnt int64
	inQueueStats := &PendingLoadStats{Type: PendingInQueue, Stats: []*EventStats{}}
	staleStats := &PendingLoadStats{Type: PendingStale, Stats: []*EventStats{}}
	pendingMigrationStats := &PendingLoadStats{Type: PendingMigration, Stats: []*EventStats{}}
	for rows.Next() {
		err = rows.Scan(&table, &version, &isStale, &minTS, &cnt)
		if err != nil {
			return nil, fmt.Errorf("error scanning event rows: %v", err)
		}
		if isStale {
			updateStats(staleStats, table, cnt, minTS)
		} else {
			currentVersion, ok := b.versions.Get(table)
			if ok && int64(currentVersion) < version {
				updateStats(pendingMigrationStats, table, cnt, minTS)
			} else {
				updateStats(inQueueStats, table, cnt, minTS)
			}
		}
	}
	pendingLoadStats := []*PendingLoadStats{inQueueStats, staleStats, pendingMigrationStats}
	return pendingLoadStats, nil
}
