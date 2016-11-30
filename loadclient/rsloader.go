package loadclient

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/twitchscience/aws_utils/common"
	"github.com/twitchscience/rs_ingester/backend"

	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/cactus/go-statsd-client/statsd"
	"github.com/twitchscience/rs_ingester/metadata"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

//RSLoader contains the redshift backend, stats module, and s3 bucket for the loader
type RSLoader struct {
	rsBackend  backend.Backend
	bucket     string
	stats      statsd.Statter
	s3Uploader s3manageriface.UploaderAPI
}

//NewRSLoader returns a RSLoader instance
func NewRSLoader(s3Uploader s3manageriface.UploaderAPI, rsBackend backend.Backend, manifestBucket string, stats statsd.Statter) (Loader, error) {
	return &RSLoader{
		rsBackend:  rsBackend,
		bucket:     manifestBucket,
		stats:      stats,
		s3Uploader: s3Uploader}, nil
}

//LoadManifest takes a load manifest object and uses the RSBackend to load the manifest into redshift
func (rsl *RSLoader) LoadManifest(manifest *metadata.LoadManifest) LoadError {
	start := time.Now()

	manifestURL, err := rsl.CreateManifestInBucket(manifest)
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

	_ = rsl.stats.Timing(manifest.TableName, int64(time.Since(start)), 1)
	return nil
}

//CheckLoad checks the status of a current manifest load into Redshift
func (rsl *RSLoader) CheckLoad(manifestUUID string) (scoop_protocol.LoadStatus, error) {
	url := manifestURL(rsl.bucket, manifestUUID)

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
func (rsl *RSLoader) CreateManifestInBucket(manifest *metadata.LoadManifest) (string, error) {
	manifestJSON, err := makeManifestJSON(manifest)
	if err != nil {
		return "", err
	}

	url := manifestURL(rsl.bucket, manifest.UUID)
	_, err = rsl.s3Uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(rsl.bucket),
		Key:    aws.String(manifest.UUID + ".json"),
		Body:   bytes.NewReader(manifestJSON),
	})

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
