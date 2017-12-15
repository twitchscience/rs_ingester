package metadata

/* Postgres-based backend */

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"sync"
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
	db             *sql.DB
	cfg            *PGConfig
	loadChecker    loadChecker
	wait           chan struct{}
	loadReady      chan *LoadManifest
	gracefulClose  chan struct{}
	versions       versions.Getter
	lastLoaded     map[string]time.Time
	lastLoadedLock sync.RWMutex
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

	logger.Info("Checking orphaned loads in PostgresLoader startup")
	err = b.checkOrphanedLoads()
	if err != nil {
		return nil, fmt.Errorf("checking orphaned loads: %s", err)
	}
	logger.Info("Done checking orphaned loads in PostgresLoader startup")

	logger.Info("Pulling table last loaded times from DB")
	ll, err := b.getLastLoadedTimes()
	if err != nil {
		return nil, fmt.Errorf("pulling table last loaded times: %s", err)
	}
	logger.Info("Done Pulling table last loaded times from DB")
	b.lastLoaded = ll

	logger.Go(b.loadReadyWorker)

	return b, nil
}

//execFnInTransaction takes a closure function of a request and runs it on redshift in a transaction
func (b *postgresBackend) execFnInTransaction(work func(*sql.Tx) error) error {
	tx, err := b.db.Begin()
	if err != nil {
		return err
	}
	err = isolateTransaction(tx)
	if err != nil {
		logger.WithError(err).Error("Error setting transaction serializable")
		return err
	}
	err = work(tx)
	if err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			logger.WithError(rollbackErr).Error("Could not rollback successfully")
		}
		return err
	}
	if commitErr := tx.Commit(); commitErr != nil {
		return fmt.Errorf("committing: %v", commitErr)
	}
	return nil
}

func (b *postgresBackend) getLastLoadedTimes() (map[string]time.Time, error) {
	rows, err := b.db.Query(`SELECT tablename, last_loaded FROM last_load`)
	if err != nil {
		return nil, err
	}

	defer func() {
		err = rows.Close()
		if err != nil {
			logger.WithError(err).Error("Error closing rows for initializing last loaded tables")
		}
	}()

	times := map[string]time.Time{}
	for rows.Next() {
		var tablename string
		var lastLoaded time.Time
		err = rows.Scan(&tablename, &lastLoaded)
		if err != nil {
			return nil, fmt.Errorf("Got error fetching last loaded row: %v", err)
		}
		if _, exists := times[tablename]; exists {
			logger.WithField("table", tablename).Error("Table appeared twice in last_loaded")
			continue
		}
		times[tablename] = lastLoaded
	}

	return times, nil
}

func (b *postgresBackend) getTableNameFromUUID(tx *sql.Tx, uuid string) (string, error) {
	rows, err := tx.Query("SELECT distinct tablename FROM tsv WHERE manifest_uuid = $1", uuid)
	if err != nil {
		return "", err
	}
	defer func() {
		err = rows.Close()
		if err != nil {
			logger.WithError(err).Error("Error closing rows for getting orphan table name")
		}
	}()

	rows.Next()
	var tableName string
	err = rows.Scan(&tableName)
	if err != nil {
		logger.WithError(err).Error("Scan threw an error")
		return "", err
	}

	return tableName, nil
}

func (b *postgresBackend) checkOrphanedLoads() error {
	rows, err := b.db.Query(`
		SELECT DISTINCT m.uuid, t.tablename
		FROM manifest m JOIN tsv t
			ON m.uuid = t.manifest_uuid
		WHERE m.retry_ts IS NULL`)
	if err != nil {
		return err
	}

	defer func() {
		err = rows.Close()
		if err != nil {
			logger.WithError(err).Error("Error closing rows for orphan load check")
		}
	}()

	orphans := map[string]string{}
	for rows.Next() {
		var uuid string
		var tablename string
		err = rows.Scan(&uuid, &tablename)
		if err != nil {
			return fmt.Errorf("querying for orphaned loads: %v", err)
		}
		orphans[uuid] = tablename
	}

	for orphanUUID, tablename := range orphans {
		loadStatus, err := b.loadChecker.CheckLoad(orphanUUID)
		if err != nil {
			return fmt.Errorf("checking orphaned load status: %s", err)
		}

		err = b.execFnInTransaction(func(tx *sql.Tx) error {
			var innerErr error
			switch loadStatus {
			case scoop_protocol.LoadComplete:
				// If completed succesfully, delete tsv rows
				logger.WithField("orphanUUID", orphanUUID).Info("Orphaned load is complete, marking done")
				innerErr = b.loadDoneHelper(tx, orphanUUID, tablename, time.Now().In(time.UTC))

			case scoop_protocol.LoadNotFound, scoop_protocol.LoadFailed:
				// If load failed, mark for retry
				logger.WithField("orphanUUID", orphanUUID).Info("Orphaned load failed, marking for retry")
				innerErr = b.loadErrorHelper(tx, orphanUUID, "Orphan load on startup")

			default:
				logger.WithField("orphanUUID", orphanUUID).WithField("loadStatus", loadStatus).Error(
					"Got unexpected load status from orphan load check")
				return fmt.Errorf("unexpected load status from orphan load check: %s", loadStatus)
			}
			return innerErr
		})
		if err != nil {
			return err
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

func (b *postgresBackend) LoadDone(manifestUUID string, tableName string) {
	doneTime := time.Now().In(time.UTC)
	err := retryInTransaction(dbRetryCount, b.db, func(tx *sql.Tx) error {
		return b.loadDoneHelper(tx, manifestUUID, tableName, doneTime)
	})
	if err != nil {
		logger.WithError(err).WithField("manifestUUID", manifestUUID).
			Error("Error marking load as done and used all retries; final error attached")
		return
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
			return nil, fmt.Errorf("fetching unloaded tsv versions: %v", err)
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

// Non-committing load done helper function
func (b *postgresBackend) loadDoneHelper(tx *sql.Tx, manifestUUID string, tableName string, doneTime time.Time) error {
	_, err := tx.Exec("DELETE FROM tsv WHERE manifest_uuid = $1", manifestUUID)
	if err != nil {
		return err
	}

	_, err = tx.Exec("DELETE FROM manifest WHERE uuid = $1", manifestUUID)
	if err != nil {
		return err
	}

	// update last_load time for the table in ingesterdb
	_, err = tx.Exec(`
		DELETE FROM last_load
		WHERE tablename = $1`, tableName)
	if err != nil {
		return err
	}
	_, err = tx.Exec(`
		INSERT INTO last_load (tablename, last_loaded)
		VALUES ($1, $2)`, tableName, doneTime)
	if err != nil {
		return err
	}

	// DB is updated, now update in-memory last load object
	b.updateLastLoad(tableName, doneTime)

	return nil
}

func (b *postgresBackend) updateLastLoad(table string, llTime time.Time) {
	b.lastLoadedLock.Lock()
	defer b.lastLoadedLock.Unlock()

	b.lastLoaded[table] = llTime
}

func (b *postgresBackend) loadErrorHelper(tx *sql.Tx, manifestUUID, loadError string) error {
	_, err := tx.Exec("UPDATE manifest SET retry_ts = $1, last_error = $2 WHERE uuid = $3",
		time.Now().In(time.UTC).Add(errorRetryDelay),
		loadError,
		manifestUUID)
	return err
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
				if err != nil {
					logger.WithError(err).Warn("Error checking failed loads")
				}
				return err
			})
			if err == nil {
				if failed != nil {
					b.loadReady <- failed
					continue
				}
				lastFailedLoadCheck = time.Now().In(time.UTC)
			} else {
				logger.WithError(err).Error("Error checking failed loads")
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

// Check for failed loads, marking them as done if they actually succeeded. If retriable, returns
// them to be added to the load queue
func (b *postgresBackend) fetchFailedLoad() (*LoadManifest, error) {
	var loadUUID, lastError string
	for { // Loop until we find a non-successful failed load, there are no more failed loads, or there was an error
		var err error

		var status scoop_protocol.LoadStatus
		err = b.execFnInTransaction(func(tx *sql.Tx) error {
			var innerErr error
			loadUUID, lastError, innerErr = failedLoadMetadata(tx)
			if loadUUID == "" || innerErr != nil { // no more failed loads or an error
				return innerErr
			}
			status, innerErr = b.loadChecker.CheckLoad(loadUUID)
			if innerErr != nil {
				return fmt.Errorf("checking load: %s", innerErr)
			}
			if status != scoop_protocol.LoadComplete {
				return nil
			}
			// This load failed but the commit went through, mark the load as
			// done and look for more failed loads to retry
			logger.WithField("loadUUID", loadUUID).
				WithField("lastError", lastError).
				Warning("failed load was discovered as having succeeded, marking as done")
			tableName, innerErr := b.getTableNameFromUUID(tx, loadUUID)
			if innerErr != nil {
				return fmt.Errorf("retrieving table name: %s", innerErr)
			}
			doneTime := time.Now().In(time.UTC)
			innerErr = b.loadDoneHelper(tx, loadUUID, tableName, doneTime)
			if innerErr != nil {
				return fmt.Errorf("load done helper: %s", innerErr)
			}
			return nil
		})
		if loadUUID == "" { // no more failed loads
			return nil, nil
		}
		if err != nil {
			return nil, err
		}

		if status != scoop_protocol.LoadComplete {
			// found a load that didn't actually fail!
			break
		}
	}

	logger.WithField("loadUUID", loadUUID).
		WithField("error", lastError).
		Infof("Load failed and has a known error, retrying manifest")
	var tsv *LoadManifest
	err := b.execFnInTransaction(func(tx *sql.Tx) error {
		var innerErr error
		tsv, innerErr = getLoadManifest(tx, loadUUID)
		return innerErr
	})
	return tsv, err
}

func failedLoadMetadata(tx *sql.Tx) (loadUUID string, lastError string, err error) {
	now := time.Now().In(time.UTC)
	rows, err := tx.Query(`
		UPDATE manifest
		SET retry_ts = null, retry_count = retry_count + 1
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
			logger.WithError(err).Warning("Error beginning transaction")
			return err
		}

		if err = isolateTransaction(tx); err != nil {
			logger.WithError(err).Warning("Error setting transaction serializable")
			return err
		}

		err = f(tx)

		if err != nil {
			logger.WithError(err).Warning("Error in transaction")
			return rollbackAndError(tx, err)
		}

		err = tx.Commit()
		if err != nil {
			logger.WithError(err).Warning("Error in commit")
			return rollbackAndError(tx, err)
		}
		return err
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

func (b *postgresBackend) IsForceLoadRequested(table string) (bool, error) {
	row := b.db.QueryRow("SELECT exists(SELECT 1 FROM force_load WHERE tablename = $1 AND started IS NULL)",
		table)
	var requested bool
	err := row.Scan(&requested)
	if err != nil {
		return false, fmt.Errorf("Checking if a table force load has been requested failed: %v", err)
	}
	return requested, nil
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

// ListDistinctTables returns all the tables in tsv
func (b *postgresBackend) ListDistinctTables() ([]string, error) {
	rows, err := b.db.Query("SELECT DISTINCT tablename FROM tsv")
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %v", err)
	}
	defer func() {
		err = rows.Close()
		if err != nil {
			logger.WithError(err).Error("error closing rows")
		}
	}()

	var table string
	var distinctTables []string

	for rows.Next() {
		err = rows.Scan(&table)
		if err != nil {
			return nil, fmt.Errorf("error scanning event rows: %v", err)
		}
		distinctTables = append(distinctTables, table)
	}
	return distinctTables, nil
}

// GetLastLoad returns all known last load times for all tables
func (b *postgresBackend) GetLastLoads() map[string]time.Time {
	b.lastLoadedLock.RLock()
	defer b.lastLoadedLock.RUnlock()

	return b.lastLoaded
}
