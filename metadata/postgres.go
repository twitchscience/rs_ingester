package metadata

/* Postgres-based backend */

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"code.google.com/p/go-uuid/uuid"
	_ "github.com/lib/pq"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

type PGConfig struct {
	DatabaseURL      string
	LoadAgeTrigger   time.Duration
	LoadCountTrigger int
	MaxConnections   int
	TableWhitelist   string
}

type LoadChecker interface {
	CheckLoad(batchUuid string) (scoop_protocol.LoadStatus, error)
}

type postgresBackend struct {
	db            *sql.DB
	cfg           *PGConfig
	loadChecker   LoadChecker
	wait          chan struct{}
	loadReady     chan *LoadBatch
	gracefulClose chan struct{}
}

var (
	noLoadsError       = errors.New("No loads were found with that batch id")
	maxLoadRetryCount  int
	dbRetryCount       int
	loadBatchTable     string
	pendingLoadTable   string
	noWorkDelay        time.Duration
	errorRetryDelay    time.Duration
	staleRetryDelay    time.Duration
	staleRecoverDelay  time.Duration
	staleCheckInterval time.Duration
)

func init() {
	flag.StringVar(&loadBatchTable, "load_batch_table", "load_batch", "Loads DB table for batches")
	flag.StringVar(&pendingLoadTable, "pending_load_table", "pending_load", "Loads DB table for loads")
	flag.DurationVar(&noWorkDelay, "no_work_delay", time.Minute, "Time to wait if there's no work to do")
	flag.IntVar(&maxLoadRetryCount, "max_load_retry", 5, "Number of times to retry a load batch before giving up")
	flag.IntVar(&dbRetryCount, "max_db_retry", 10, "Number of times to retry a transaction")
	flag.DurationVar(&errorRetryDelay, "error_retry_delay", time.Minute*15, "Time to wait to retry a load that errors")
	flag.DurationVar(&staleRetryDelay, "stale_retry_delay", time.Hour*3, "Time to wait before checking up on a load")
	flag.DurationVar(&staleRecoverDelay, "stale_recover_delay", time.Minute*30, "Time to check out a stale load (lock duration)")
	flag.DurationVar(&staleCheckInterval, "stale_check_interval", time.Minute, "How often to check for stale loads")
}

// Configure a new postgres backend for storing only
func NewPostgresStorer(cfg *PGConfig) (MetadataStorer, error) {
	b := &postgresBackend{
		cfg:       cfg,
		loadReady: nil,
		wait:      make(chan struct{}),
	}

	err := b.connectToDB()
	if err != nil {
		return nil, err
	}

	return b, nil
}

// Configure a new postgres backend for loading (or storing)
// At backend configuration, we set a max number of pending loads for a table
// and max count of pending loads before a load is triggered.
func NewPostgresLoader(cfg *PGConfig, loadChecker LoadChecker) (MetadataBackend, error) {
	b := &postgresBackend{
		cfg:           cfg,
		loadChecker:   loadChecker,
		loadReady:     make(chan *LoadBatch),
		wait:          make(chan struct{}),
		gracefulClose: make(chan struct{}),
	}

	err := b.connectToDB()
	if err != nil {
		return nil, err
	}

	go b.loadReadyWorker()

	return b, nil
}

func (b *postgresBackend) InsertLoad(load *Load) error {
	_, err := b.db.Exec(
		"INSERT INTO "+pendingLoadTable+" (tablename, keyname, ts) VALUES ($1, $2, $3)",
		load.TableName,
		load.KeyName,
		time.Now().In(time.UTC),
	)
	return err
}

func (b *postgresBackend) LoadReady() chan *LoadBatch {
	return b.loadReady
}

func (b *postgresBackend) LoadDone(batchUuid string) {
	err := retryInTransaction(dbRetryCount, b.db, func(tx *sql.Tx) error {
		return b.loadDoneHelper(tx, batchUuid)
	})
	if err != nil {
		log.Printf("Error marking load %s as done and used all retries; final error: %s\n", batchUuid, err.Error())
	}
}

func (b *postgresBackend) LoadError(batchUuid string, loadError string) {
	err := retryInTransaction(dbRetryCount, b.db, func(tx *sql.Tx) error {
		return b.loadErrorHelper(tx, batchUuid, loadError)
	})
	if err != nil {
		log.Printf("Error marking load %s as error and used all retries; final error: %s\n", batchUuid, err.Error())
	}
}

// Close the backend; signals the loadready worker to end gracefully if it is running
func (b *postgresBackend) Close() {
	close(b.wait)
	if b.gracefulClose != nil {
		<-b.gracefulClose
	}
}

// Non-commiting load done helper function
func (b *postgresBackend) loadDoneHelper(tx *sql.Tx, batchUuid string) error {
	_, err := tx.Exec("DELETE FROM "+pendingLoadTable+" WHERE batch_uuid = $1", batchUuid)
	if err != nil {
		return rollbackAndError(tx, err)
	}

	_, err = tx.Exec("DELETE FROM "+loadBatchTable+" WHERE uuid = $1", batchUuid)
	if err != nil {
		return rollbackAndError(tx, err)
	}

	return nil
}

func (b *postgresBackend) loadErrorHelper(tx *sql.Tx, batchUuid, loadError string) error {
	_, err := tx.Exec("UPDATE "+loadBatchTable+" SET retry_ts = $1, last_error = $2 WHERE uuid = $3",
		time.Now().In(time.UTC).Add(errorRetryDelay),
		loadError,
		batchUuid)
	if err != nil {
		return rollbackAndError(tx, err)
	}

	return nil
}

func (b *postgresBackend) loadReadyWorker() {
	var lastStaleCheck time.Time
	for {
		var stale *LoadBatch

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

		var batch *LoadBatch

		retrying(dbRetryCount, func() error {
			var err error
			batch, err = b.fetchLoad()
			return err
		})

		sleepDelay := noWorkDelay
		if batch != nil {
			b.loadReady <- batch
			sleepDelay = time.Millisecond * 10
		}

		select {
		case <-b.wait:
			close(b.loadReady)
			close(b.gracefulClose)
			return
		case <-time.After(sleepDelay):
		}
	}
}

// Check for dead loads and clean them up if possible
func (b *postgresBackend) fetchStaleLoad() (*LoadBatch, error) {
	for {
		tx, err := b.db.Begin()
		if err != nil {
			log.Println("Error beginning transaction", err)
			return nil, err
		}

		if err := isolateTransaction(tx); err != nil {
			log.Println("Error setting transaction serializable", err)
			return nil, err
		}

		loadUuid, lastError, err := staleLoadMetadata(tx)
		if err != nil {
			return nil, err
		}

		if loadUuid == "" {
			return nil, nil
		}

		log.Println("Checking up on load", loadUuid)

		err = tx.Commit()
		if err != nil {
			log.Println("Error on commit when locking batch load", err)
			return nil, err
		}

		loadStatus, err := b.loadChecker.CheckLoad(loadUuid)
		if err != nil {
			log.Println("Got error checking load status", err)
			return nil, err // Is exiting the best thing to do here?
		}

		tx, err = b.db.Begin()
		if err != nil {
			log.Println("Error beginning transaction", err)
			return nil, err
		}

		if err := isolateTransaction(tx); err != nil {
			log.Println("Error setting transaction serializable", err)
			return nil, err
		}

		switch loadStatus {
		case scoop_protocol.LoadComplete:
			// If completed succesfully, delete pending rows
			log.Printf("Load %s is complete, marking done", loadUuid)
			err = b.loadDoneHelper(tx, loadUuid)
			if err != nil {
				return nil, err
			}

			err = tx.Commit()

		case scoop_protocol.LoadNotFound, scoop_protocol.LoadFailed:
			if lastError.Valid {
				log.Printf("Load %s is in status %s and has a known error (%s), retrying batch", loadUuid, loadStatus, lastError.String)
				loadBatch, err := getLoadBatch(tx, loadUuid)
				if err != nil {
					return nil, rollbackAndError(tx, err)
				}
				return loadBatch, tx.Commit()
			}

			log.Printf("Load %s is in status %s, deleting batch so it retries", loadUuid, loadStatus)
			_, err = tx.Exec(`UPDATE `+pendingLoadTable+` SET batch_uuid = NULL
                              WHERE batch_uuid = $1`, loadUuid)
			if err != nil {
				return nil, rollbackAndError(tx, err)
			}
			_, err = tx.Exec("DELETE FROM "+loadBatchTable+" WHERE uuid = $1", loadUuid)
			if err != nil {
				return nil, rollbackAndError(tx, err)
			}

			err := tx.Commit()
			if err != nil {
				return nil, err
			}

		case scoop_protocol.LoadInProgress:
			log.Printf("Load %s is still in progress, waiting\n", loadUuid)
			err = tx.Rollback()
			if err != nil {
				return nil, err
			}
		}
	}
}

func staleLoadMetadata(tx *sql.Tx) (loadUuid string, lastError sql.NullString, err error) {
	now := time.Now().In(time.UTC)
	retry_ts := time.Now().In(time.UTC).Add(staleRecoverDelay)
	rows, err := tx.Query(`UPDATE `+loadBatchTable+`
                               SET retry_ts = $1,
                                   retry_count = retry_count + 1
                               WHERE uuid IN (
                                 SELECT uuid
                                 FROM `+loadBatchTable+`
                                 WHERE retry_ts < $2 AND retry_count < $3
                                 ORDER BY retry_ts ASC
                                 LIMIT 1
                               )
                               RETURNING uuid, last_error
                              `, retry_ts, now, maxLoadRetryCount)

	if err != nil {
		log.Println("Error querying for stale locks", err)
		err = rollbackAndError(tx, err)
		return
	}
	defer rows.Close()

	if rows.Next() {
		err = rows.Scan(&loadUuid, &lastError)
		if err != nil {
			log.Println("Got error fetching pending load", err)
			err = rollbackAndError(tx, err)
			return
		}
	} else {
		// No work left to do
		err = tx.Commit()
	}
	return
}

func (b *postgresBackend) connectToDB() error {
	var err error
	b.db, err = sql.Open("postgres", b.cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("Got err %v while connecting to rds", err)
	}
	err = b.db.Ping()
	if err != nil {
		return fmt.Errorf("Could not ping rds %v", err)
	}
	b.db.SetMaxOpenConns(b.cfg.MaxConnections)
	return nil
}

func (b *postgresBackend) PingDB() error {
	err := b.db.Ping()
	if err != nil {
		return fmt.Errorf("Could not ping rds %v", err)
	}
	return nil
}

func (b *postgresBackend) fetchLoad() (*LoadBatch, error) {
	tx, err := b.db.Begin()
	if err != nil {
		return nil, err
	}

	if isolateTransaction(tx); err != nil {
		log.Println("Error on setting serializable:", err)
		return nil, rollbackAndError(tx, err)
	}

	batchUuid := uuid.NewRandom().String()
	retryTs := time.Now().In(time.UTC).Add(staleRetryDelay)

	_, err = tx.Exec(`INSERT INTO `+loadBatchTable+` (uuid, retry_ts)
                      VALUES ($1, $2)
                     `, batchUuid, retryTs)
	if err != nil {
		log.Println("Error creating "+loadBatchTable, err)
		return nil, rollbackAndError(tx, err)
	}

	tableWhitelistClause := ""

	if strings.Trim(b.cfg.TableWhitelist, "\t\n ") != "" {
		tableWhitelistClause = " AND tablename IN ('" + strings.Replace(b.cfg.TableWhitelist, ",", "', '", -1) + "')"
	}

	_, err = tx.Exec(
		`UPDATE `+pendingLoadTable+` SET batch_uuid = $1
         WHERE tablename IN
         (SELECT tablename FROM
            (SELECT tablename, min(ts) AS oldest, count(*) AS cnt
             FROM `+pendingLoadTable+` WHERE batch_uuid IS NULL
             GROUP BY tablename) a
          WHERE (cnt > $2 OR oldest < $3)
          `+tableWhitelistClause+`
          ORDER BY oldest
          LIMIT 1
          )
         AND batch_uuid IS NULL
        `,
		batchUuid,
		b.cfg.LoadCountTrigger,
		time.Now().In(time.UTC).Add(-b.cfg.LoadAgeTrigger),
	)

	if err != nil {
		return nil, rollbackAndError(tx, err)
	}

	loadBatch, err := getLoadBatch(tx, batchUuid)
	if err != nil {
		if err == noLoadsError {
			return nil, tx.Rollback()
		} else {
			return nil, rollbackAndError(tx, err)
		}
	}

	return loadBatch, tx.Commit()
}

func getLoadBatch(tx *sql.Tx, batchUuid string) (*LoadBatch, error) {
	var batch LoadBatch
	batch.UUID = batchUuid

	rows, err := tx.Query("SELECT keyname, tablename FROM "+pendingLoadTable+" WHERE batch_uuid = $1", batchUuid)
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	for rows.Next() {
		var load Load
		err := rows.Scan(&load.KeyName, &load.TableName)
		if err != nil {
			log.Println("Scan threw an error!")
			return nil, err
		}

		batch.Loads = append(batch.Loads, load)
	}

	if len(batch.Loads) == 0 {
		return nil, noLoadsError
	}

	batch.TableName = batch.Loads[0].TableName

	return &batch, nil
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

		if err := isolateTransaction(tx); err != nil {
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
	_, err = tx.Exec("LOCK TABLE " + pendingLoadTable + ", " + loadBatchTable + ";")
	return err
}

func rollbackAndError(tx *sql.Tx, err error) error {
	new_err := tx.Rollback()
	if new_err != nil {
		return fmt.Errorf("Rollback error (%v); previous error (%v)", new_err, err)
	}
	return err
}
