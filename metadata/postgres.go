package metadata

/* Postgres-based backend */

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"time"

	_ "github.com/lib/pq" // To register "postgres" with database/sql
	"github.com/pborman/uuid"
	"github.com/twitchscience/rs_ingester/constants"
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
	errorNoLoads          = errors.New("No loads were found with that manifest id")
	tableToLoadSearchSize = 50
	maxLoadRetryCount     int
	dbRetryCount          int
	noWorkDelay           time.Duration
	errorRetryDelay       time.Duration
	staleRetryDelay       time.Duration
	staleRecoverDelay     time.Duration
	staleCheckInterval    time.Duration
)

func init() {
	flag.DurationVar(&noWorkDelay, "no_work_delay", time.Minute, "Time to wait if there's no work to do")
	flag.IntVar(&maxLoadRetryCount, "max_load_retry", 5, "Number of times to retry a load manifest before giving up")
	flag.IntVar(&dbRetryCount, "max_db_retry", 10, "Number of times to retry a transaction")
	flag.DurationVar(&errorRetryDelay, "error_retry_delay", time.Minute*15, "Time to wait to retry a load that errors")
	flag.DurationVar(&staleRetryDelay, "stale_retry_delay", time.Hour*3, "Time to wait before checking up on a load")
	flag.DurationVar(&staleRecoverDelay, "stale_recover_delay", time.Minute*30, "Time to check out a stale load (lock duration)")
	flag.DurationVar(&staleCheckInterval, "stale_check_interval", time.Minute, "How often to check for stale loads")
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

	go b.loadReadyWorker()

	return b, nil
}

func (b *postgresBackend) InsertLoad(load *Load) error {
	_, err := b.db.Exec(
		"INSERT INTO "+constants.TsvTable+" (tablename, keyname, tableversion, ts) VALUES ($1, $2, $3, $4)",
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
		log.Printf("Error marking load %s as done and used all retries; final error: %s\n", manifestUUID, err.Error())
	}
}

func (b *postgresBackend) LoadError(manifestUUID string, loadError string) {
	err := retryInTransaction(dbRetryCount, b.db, func(tx *sql.Tx) error {
		return b.loadErrorHelper(tx, manifestUUID, loadError)
	})
	if err != nil {
		log.Printf("Error marking load %s as error and used all retries; final error: %s\n", manifestUUID, err.Error())
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
			log.Printf("Error closing rows for versions: %s", err)
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
	_, err := tx.Exec("DELETE FROM "+constants.TsvTable+" WHERE manifest_uuid = $1", manifestUUID)
	if err != nil {
		return rollbackAndError(tx, err)
	}

	_, err = tx.Exec("DELETE FROM "+constants.ManifestTable+" WHERE uuid = $1", manifestUUID)
	if err != nil {
		return rollbackAndError(tx, err)
	}

	return nil
}

func (b *postgresBackend) loadErrorHelper(tx *sql.Tx, manifestUUID, loadError string) error {
	_, err := tx.Exec("UPDATE "+constants.ManifestTable+" SET retry_ts = $1, last_error = $2 WHERE uuid = $3",
		time.Now().In(time.UTC).Add(errorRetryDelay),
		loadError,
		manifestUUID)
	if err != nil {
		return rollbackAndError(tx, err)
	}

	return nil
}

func (b *postgresBackend) loadReadyWorker() {
	var lastStaleCheck time.Time
	for {
		var stale *LoadManifest

		if time.Now().In(time.UTC).Sub(lastStaleCheck) > staleCheckInterval {
			err := retrying(dbRetryCount, func() error {
				var err error
				stale, err = b.fetchStaleLoad()
				return err
			})
			if err == nil {
				lastStaleCheck = time.Now().In(time.UTC)
			} else {
				log.Println("Error checking stale loads:", err)
			}

			if stale != nil {
				b.loadReady <- stale
			}
		}

		var manifest *LoadManifest

		err := retrying(dbRetryCount, func() error {
			var err error
			manifest, err = b.fetchLoad()
			return err
		})
		if err != nil {
			log.Printf("Error fetching manifest for load after %d tries. last error: %s", dbRetryCount, err)
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

// Check for dead loads and clean them up if possible
func (b *postgresBackend) fetchStaleLoad() (*LoadManifest, error) {
	for {
		tx, err := b.db.Begin()
		if err != nil {
			log.Println("Error beginning transaction", err)
			return nil, err
		}

		if err = isolateTransaction(tx); err != nil {
			log.Println("Error setting transaction serializable", err)
			return nil, err
		}

		loadUUID, lastError, err := staleLoadMetadata(tx)
		if err != nil {
			return nil, err
		}

		if loadUUID == "" {
			return nil, nil
		}

		log.Println("Checking up on load", loadUUID)

		err = tx.Commit()
		if err != nil {
			log.Println("Error on commit when locking manifest load", err)
			return nil, err
		}

		loadStatus, err := b.loadChecker.CheckLoad(loadUUID)
		if err != nil {
			log.Println("Got error checking load status", err)
			return nil, err // Is exiting the best thing to do here?
		}

		tx, err = b.db.Begin()
		if err != nil {
			log.Println("Error beginning transaction", err)
			return nil, err
		}

		if err = isolateTransaction(tx); err != nil {
			log.Println("Error setting transaction serializable", err)
			return nil, err
		}

		switch loadStatus {
		case scoop_protocol.LoadComplete:
			// If completed succesfully, delete tsv rows
			log.Printf("Load %s is complete, marking done", loadUUID)
			err = b.loadDoneHelper(tx, loadUUID)
			if err != nil {
				return nil, err
			}

			err = tx.Commit()

		case scoop_protocol.LoadNotFound, scoop_protocol.LoadFailed:
			if lastError.Valid {
				log.Printf("Load %s is in status %s and has a known error (%s), retrying manifest", loadUUID, loadStatus, lastError.String)
				var tsv *LoadManifest
				tsv, err = getLoadManifest(tx, loadUUID)
				if err != nil {
					return nil, rollbackAndError(tx, err)
				}
				return tsv, tx.Commit()
			}

			log.Printf("Load %s is in status %s, deleting manifest so it retries", loadUUID, loadStatus)
			_, err = tx.Exec(`UPDATE `+constants.TsvTable+` SET manifest_uuid = NULL
                              WHERE manifest_uuid = $1`, loadUUID)
			if err != nil {
				return nil, rollbackAndError(tx, err)
			}
			_, err = tx.Exec("DELETE FROM "+constants.ManifestTable+" WHERE uuid = $1", loadUUID)
			if err != nil {
				return nil, rollbackAndError(tx, err)
			}

			err = tx.Commit()
			if err != nil {
				return nil, err
			}

		case scoop_protocol.LoadInProgress:
			log.Printf("Load %s is still in progress, waiting\n", loadUUID)
			err = tx.Rollback()
			if err != nil {
				return nil, err
			}
		}
	}
}

func staleLoadMetadata(tx *sql.Tx) (loadUUID string, lastError sql.NullString, err error) {
	now := time.Now().In(time.UTC)
	retryTs := now.Add(staleRecoverDelay)
	rows, err := tx.Query(`UPDATE `+constants.ManifestTable+`
                               SET retry_ts = $1,
                                   retry_count = retry_count + 1
                               WHERE uuid IN (
                                 SELECT uuid
                                 FROM `+constants.ManifestTable+`
                                 WHERE retry_ts < $2 AND retry_count < $3
                                 ORDER BY retry_ts ASC
                                 LIMIT 1
                               )
                               RETURNING uuid, last_error
                              `, retryTs, now, maxLoadRetryCount)

	if err != nil {
		log.Println("Error querying for stale locks", err)
		err = rollbackAndError(tx, err)
		return
	}
	defer func() {
		err = rows.Close()
		if err != nil {
			log.Printf("Error closing rows for stale load metadata: %s", err)
		}
	}()

	if rows.Next() {
		err = rows.Scan(&loadUUID, &lastError)
		if err != nil {
			log.Println("Got error fetching tsv row", err)
			err = rollbackAndError(tx, err)
			return
		}
	} else {
		// No work left to do
		err = tx.Commit()
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

func (b *postgresBackend) findTableVersionToLoad(tx *sql.Tx) (string, int, error) {
	rows, err := tx.Query(fmt.Sprintf(`
SELECT tablename, tableversion FROM
	(SELECT tablename, tableversion, min(ts) AS oldest, count(*) AS cnt
		FROM %s WHERE manifest_uuid IS NULL
		GROUP BY tablename, tableversion) a
WHERE (cnt > $1 OR oldest < $2)
ORDER BY oldest ASC
LIMIT $3
`, constants.TsvTable),
		b.cfg.LoadCountTrigger,
		time.Now().In(time.UTC).Add(-b.cfg.LoadAgeTrigger),
		tableToLoadSearchSize,
	)
	if err != nil {
		return "", -1, fmt.Errorf("Error finding potential tables to load: %v", err)
	}

	defer func() {
		err = rows.Close()
		if err != nil {
			log.Printf("Error closing rows for findTableVersionToLoad: %s", err)
		}
	}()

	var table string
	var version int
	found := false
	for rows.Next() && !found {
		if err = rows.Scan(&table, &version); err != nil {
			return "", -1, fmt.Errorf("Error parsing rows when looking for potential tables to load: %v", err)
		}
		currentVersion, exists := b.versions.Get(table)
		if exists && version <= currentVersion {
			found = true
		}
	}
	if !found {
		return "", -1, fmt.Errorf("Searched %d (table, version) pairs to load and found none appropriate.", tableToLoadSearchSize)
	}
	return table, version, nil
}

func (b *postgresBackend) fetchLoad() (*LoadManifest, error) {
	tx, err := b.db.Begin()
	if err != nil {
		return nil, err
	}

	if err = isolateTransaction(tx); err != nil {
		log.Println("Error on setting serializable:", err)
		return nil, rollbackAndError(tx, err)
	}

	manifestUUID := uuid.NewRandom().String()
	retryTs := time.Now().In(time.UTC).Add(staleRetryDelay)

	_, err = tx.Exec(`INSERT INTO `+constants.ManifestTable+` (uuid, retry_ts)
                      VALUES ($1, $2)
                     `, manifestUUID, retryTs)
	if err != nil {
		log.Println("Error inserting into "+constants.ManifestTable, err)
		return nil, rollbackAndError(tx, err)
	}

	table, version, err := b.findTableVersionToLoad(tx)
	if err != nil {
		return nil, rollbackAndError(tx, err)
	}

	_, err = tx.Exec(
		`UPDATE `+constants.TsvTable+` SET manifest_uuid = $1
         WHERE tablename = $2
		 AND tableversion <= $3
         AND manifest_uuid IS NULL
        `,
		manifestUUID,
		table,
		version,
	)

	if err != nil {
		return nil, rollbackAndError(tx, err)
	}

	tsv, err := getLoadManifest(tx, manifestUUID)
	if err != nil {
		if err == errorNoLoads {
			return nil, tx.Rollback()
		}
		return nil, rollbackAndError(tx, err)
	}

	return tsv, tx.Commit()
}

func getLoadManifest(tx *sql.Tx, manifestUUID string) (*LoadManifest, error) {
	var manifest LoadManifest
	manifest.UUID = manifestUUID

	rows, err := tx.Query("SELECT keyname, tablename FROM "+constants.TsvTable+" WHERE manifest_uuid = $1", manifestUUID)
	if err != nil {
		return nil, err
	}

	defer func() {
		err = rows.Close()
		if err != nil {
			log.Printf("Error closing rows for load manifest: %s", err)
		}
	}()
	for rows.Next() {
		var load Load
		err := rows.Scan(&load.KeyName, &load.TableName)
		if err != nil {
			log.Printf("Scan threw an error: %v", err)
			return nil, err
		}

		manifest.Loads = append(manifest.Loads, load)
	}

	if len(manifest.Loads) == 0 {
		return nil, errorNoLoads
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
	return retrying(dbRetryCount, func() error {
		tx, err := db.Begin()
		if err != nil {
			log.Println("Error beginning transaction", err)
			return err
		}

		if err = isolateTransaction(tx); err != nil {
			log.Println("Error setting transaction serializable", err)
			return err
		}

		err = f(tx)

		if err != nil {
			log.Printf("Error in transaction %s", err.Error())
			return rollbackAndError(tx, err)
		}

		return tx.Commit()
	})
}

func isolateTransaction(tx *sql.Tx) error {
	_, err := tx.Exec("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
	if err != nil {
		return err
	}
	_, err = tx.Exec("LOCK TABLE " + constants.TsvTable + ", " + constants.ManifestTable + ";")
	return err
}

func rollbackAndError(tx *sql.Tx, err error) error {
	newErr := tx.Rollback()
	if newErr != nil {
		return fmt.Errorf("Rollback error (%v); previous error (%v)", newErr, err)
	}
	return err
}
