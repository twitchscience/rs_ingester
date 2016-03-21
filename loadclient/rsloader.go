package loadclient

import (
	"encoding/json"
	"fmt"

	"github.com/twitchscience/aws_utils/common"
	"github.com/twitchscience/rs_ingester/backend"

	"time"

	"github.com/AdRoll/goamz/s3"
	"github.com/cactus/go-statsd-client/statsd"
	"github.com/twitchscience/rs_ingester/metadata"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

//RSLoader contains the redshift backend, stats module, and s3 bucket for the loader
type RSLoader struct {
	rsBackend *backend.RedshiftBackend
	bucket    *s3.Bucket
	stats     statsd.Statter
}

//NewRSLoader returns a RSLoader provided a RSBackend, bucket, and stats receiver
func NewRSLoader(rsBackend *backend.RedshiftBackend, manifestBucketPrefix string, stats statsd.Statter) (Loader, error) {
	bucket, err := GetBucket(manifestBucketPrefix)
	if err != nil {
		return nil, err
	}

	return &RSLoader{rsBackend: rsBackend,
		bucket: bucket,
		stats:  stats}, nil
}

//LoadManifest takes a load manifest object and uses the RSBackend to load the manifest into redshift
func (rsl *RSLoader) LoadManifest(manifest *metadata.LoadManifest) LoadError {
	start := time.Now()

	manifestURL, err := CreateManifestInBucket(manifest, rsl.bucket)
	if err != nil {
		return &loadError{msg: err.Error(), isRetryable: true}
	}

	err = rsl.rsBackend.ManifestCopy(&scoop_protocol.ManifestRowCopyRequest{
		ManifestURL: manifestURL,
		TableName:   manifest.TableName,
	})
	if err != nil {
		return &loadError{msg: err.Error(), isRetryable: true}
	}

	_ = rsl.stats.Timing(manifest.TableName, int64(time.Now().Sub(start)), 1)
	return nil
}

//CheckLoad checks the status of a current manifest load into Redshift
func (rsl *RSLoader) CheckLoad(manifestUUID string) (scoop_protocol.LoadStatus, error) {
	url := manifestURL(rsl.bucket.Name, manifestUUID)

	loadstatus, err := rsl.rsBackend.LoadCheck(&scoop_protocol.LoadCheckRequest{
		ManifestURL: url,
	})
	if err != nil {
		return "", fmt.Errorf("Check load failed: %s", err.Error())
	}

	return loadstatus.LoadStatus, nil
}

//HealthCheck Checks to see if the connection to Redshift is still healthy
func (rsl *RSLoader) HealthCheck() error {
	return rsl.rsBackend.HealthCheck()
}

//CreateManifestInBucket takes a load manifest, converts into json, and loads it into a provided s3 bucket
func CreateManifestInBucket(manifest *metadata.LoadManifest, bucket *s3.Bucket) (string, error) {
	manifestJSON, err := makeManifestJSON(manifest)
	if err != nil {
		return "", err
	}

	url := manifestURL(bucket.Name, manifest.UUID)
	err = bucket.Put(manifest.UUID+".json", manifestJSON, "application/json", s3.BucketOwnerRead, s3.Options{})
	if err != nil {
		return "", err
	}

	return url, err
}

func makeManifestJSON(mani *metadata.LoadManifest) ([]byte, error) {
	m := manifest{}
	for _, k := range mani.Loads {
		m.Entries = append(m.Entries,
			entry{URL: common.NormalizeS3URL(k.KeyName),
				Mandatory: true},
		)
	}

	return json.Marshal(m)
}

func manifestURL(bucketName, uuid string) string {
	return common.NormalizeS3URL(bucketName + "/" + uuid + ".json")
}
