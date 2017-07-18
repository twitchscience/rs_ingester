package redshift

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/lib/pq"
	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

const (
	// need to provide creds, and lib/pq barfs on paramater insertion in copy commands
	copyCommand             = `COPY %s FROM %s WITH CREDENTIALS '%s' %s`
	copyCommandSearch       = `COPY %% FROM '%s' %%`
	credentialExpiryTimeout = 2 * time.Minute
)

var (
	manifestImportOptions = strings.Join([]string{
		"removequotes",
		"delimiter '\\t'",
		"gzip",
		"escape",
		"truncatecolumns",
		"roundec",
		"fillrecord",
		"compupdate on",
		"emptyasnull",
		"acceptinvchars '?'",
		"manifest",
		"trimblanks;"},
		" ",
	)
	lastCredentialExpiry = time.Now()
)

//ManifestRowCopyRequest is the redshift package's represntation of the manifest row copy object for a manifest row copy
type ManifestRowCopyRequest struct {
	BuiltOn     time.Time
	Name        string
	ManifestURL string
	Credentials string
}

//TxExec runs the execution of the manifest row copy request in a transaction
func (r ManifestRowCopyRequest) TxExec(t *sql.Tx) error {
	if strings.ContainsRune(r.ManifestURL, '\000') {
		return fmt.Errorf("ManifestURL contains a null byte")
	}
	if strings.ContainsRune(r.Name, '\000') {
		return fmt.Errorf("Name contains a null byte")
	}

	query := fmt.Sprintf(copyCommand, pq.QuoteIdentifier(r.Name),
		EscapePGString(r.ManifestURL), r.Credentials, manifestImportOptions)

	_, err := t.Exec(query)
	return err
}

//CheckLoadStatus checks the status of a load into redshift
func CheckLoadStatus(t *sql.Tx, manifestURL string) (scoop_protocol.LoadStatus, error) {
	var count int
	q := fmt.Sprintf(copyCommandSearch, manifestURL)

	err := t.QueryRow("SELECT count(*) FROM STV_RECENTS WHERE query ILIKE $1 AND status != 'Done'", q).Scan(&count)
	if err != nil {
		return "", err
	}

	if count != 0 {
		// We do this check on ingester start-up, which means that if a query is still running, the previous ingester
		// is no longer alive to issue the COMMIT, causing this load to never complete. Thus we say this is a failed load.
		logger.WithField("manifestURL", manifestURL).Info("CheckLoadStatus: Manifest copy is in STV_RECENTS as running")
		return scoop_protocol.LoadFailed, nil
	}

	var aborted, xid int
	err = t.QueryRow("SELECT xid, aborted FROM STL_QUERY WHERE querytxt ILIKE $1", q).Scan(&xid, &aborted)
	switch {
	case err == sql.ErrNoRows:
		logger.WithField("manifestURL", manifestURL).Warning("CheckLoadStatus: Manifest copy does not have a transaction ID")
		return scoop_protocol.LoadNotFound, nil
	case err != nil:
		return "", err
	default:
	}

	if aborted == 1 {
		logger.WithField("manifestURL", manifestURL).Info("CheckLoadStatus: Manifest copy was aborted while running")
		return scoop_protocol.LoadFailed, nil
	}

	err = t.QueryRow("SELECT count(*) FROM STL_UTILITYTEXT WHERE xid = $1 AND text = 'COMMIT'", xid).Scan(&count)
	if err != nil {
		return "", err
	}

	if count != 0 {
		logger.WithField("manifestURL", manifestURL).Info("CheckLoadStatus: Manifest copy was committed")
		return scoop_protocol.LoadComplete, nil
	}

	logger.WithField("manifestURL", manifestURL).Info("CheckLoadStatus: Manifest copy was found, but was not commited")
	return scoop_protocol.LoadFailed, nil
}

//CopyCredentials refreshes the redshift aws auth token aggressively
func CopyCredentials(credentials *credentials.Credentials) (accessCreds string) {
	// Agressively refresh the token
	if time.Since(lastCredentialExpiry) > credentialExpiryTimeout {
		credentials.Expire()
	}

	v, err := credentials.Get()
	if err != nil {
		logger.WithError(err).Error("Failed to retrieve credentials")
		return ""
	}

	if len(v.SessionToken) == 0 {
		accessCreds = fmt.Sprintf(
			"aws_access_key_id=%s;aws_secret_access_key=%s",
			v.AccessKeyID,
			v.SecretAccessKey,
		)
	} else {
		accessCreds = fmt.Sprintf(
			"aws_access_key_id=%s;aws_secret_access_key=%s;token=%s",
			v.AccessKeyID,
			v.SecretAccessKey,
			v.SessionToken,
		)
	}
	return
}
