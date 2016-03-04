//Client to scoop to manage loads

package loadclient

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"

	"time"

	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/s3"
	"github.com/cactus/go-statsd-client/statsd"
	"github.com/twitchscience/aws_utils/common"
	"github.com/twitchscience/aws_utils/environment"
	"github.com/twitchscience/rs_ingester/metadata"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

const (
	scoopTimeout = time.Minute * 15
)

type entry struct {
	URL       string `json:"url"`
	Mandatory bool   `json:"mandatory"`
}

type manifest struct {
	Entries []entry `json:"entries"`
}

// ScoopLoader manages loads with scoop
type ScoopLoader struct {
	scoopURL   string
	bucket     *s3.Bucket
	stats      statsd.Statter
	httpClient *http.Client
}

type scoopLoadError struct {
	msg         string
	isRetryable bool
}

func (e scoopLoadError) Error() string {
	return e.msg
}

func (e scoopLoadError) Retryable() bool {
	return e.isRetryable
}

// NewScoopLoader initializes a scoop loader
func NewScoopLoader(scoopURL, manifestBucketPrefix string, stats statsd.Statter) (Loader, error) {
	if scoopURL == "" {
		return nil, fmt.Errorf("Scoop URL must be provided")
	}

	bucket, err := GetBucket(manifestBucketPrefix)
	if err != nil {
		return nil, err
	}

	httpClient := &http.Client{Timeout: scoopTimeout}

	return &ScoopLoader{scoopURL: scoopURL,
		bucket:     bucket,
		httpClient: httpClient,
		stats:      stats}, nil
}

// LoadManifest creates and attempts to load a manifest of files
func (sl *ScoopLoader) LoadManifest(manifest *metadata.LoadManifest) LoadError {
	start := time.Now()

	manifestURL, err := CreateManifestInBucket(manifest, sl.bucket)
	if err != nil {
		return &scoopLoadError{msg: err.Error(), isRetryable: true}
	}

	req := &scoop_protocol.ManifestRowCopyRequest{ManifestURL: manifestURL, TableName: manifest.TableName}

	jsonRequest, err := json.Marshal(req)
	if err != nil {
		return &scoopLoadError{msg: err.Error(), isRetryable: true}
	}

	resp, err := sl.httpClient.Post(sl.scoopURL+"/rows/manifest_copy", "application/json", bytes.NewReader(jsonRequest))
	if err != nil {
		return &scoopLoadError{msg: err.Error(), isRetryable: false} // We just let it go stale in this case; network failures aren't safe
	}
	defer func() {
		err = resp.Body.Close()
		if err != nil {
			log.Printf("Error closing response body: %s", err)
		}
	}()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return &scoopLoadError{msg: err.Error(), isRetryable: false} // Likewise here
	}

	if resp.StatusCode != http.StatusOK {
		log.Printf("Post failed with status code: %s, body: %s", resp.Status, body)
		return &scoopLoadError{msg: fmt.Sprintf("Post failed with status code: %s, body: %s", resp.Status, body), isRetryable: true}
	}
	err = sl.stats.Timing(req.TableName, int64(time.Now().Sub(start)), 1)
	if err != nil {
		log.Printf("Error sending %s stat to statsd: %s", req.TableName, err)
	}

	return nil
}

func manifestURL(bucketName, uuid string) string {
	return common.NormalizeS3URL(bucketName + "/" + uuid + ".json")
}

// CheckLoad checks scoop for the status of a particular manifest load
func (sl *ScoopLoader) CheckLoad(manifestUUID string) (scoop_protocol.LoadStatus, error) {
	url := manifestURL(sl.bucket.Name, manifestUUID)

	rawRequest := &scoop_protocol.LoadCheckRequest{ManifestURL: url}
	request, err := json.Marshal(rawRequest)
	if err != nil {
		return "", err
	}

	resp, err := sl.httpClient.Post(sl.scoopURL+"/rows/copy/check", "application/json", bytes.NewReader(request))
	if err != nil {
		return "", err
	}

	defer func() {
		err = resp.Body.Close()
		if err != nil {
			log.Printf("Error closing response body: %s", err)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		log.Printf("Post failed with status code: %s", resp.Status)
		return "", fmt.Errorf("Post failed with status code: %s", resp.Status)
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	response := &scoop_protocol.LoadCheckResponse{}
	err = json.Unmarshal(b, response)
	if err != nil {
		return "", err
	}

	return response.LoadStatus, nil
}

// PingScoopHealthcheck hits and parses scoop's health check
func (sl *ScoopLoader) PingScoopHealthcheck() (*scoop_protocol.ScoopHealthCheck, error) {
	resp, err := sl.httpClient.Get(sl.scoopURL + "/health")
	if err != nil {
		return nil, err
	}

	defer func() {
		err = resp.Body.Close()
		if err != nil {
			log.Printf("Error closing response body: %s", err)
		}
	}()

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	response := &scoop_protocol.ScoopHealthCheck{}
	err = json.Unmarshal(b, response)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return response, errors.New("Scoop health check failed.")
	}
	return response, nil
}

// CreateManifestInBucket creates and uploads a manifest file to s3
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

// GetBucket creates the s3.Bucket from the bucket name
func GetBucket(bucketPrefix string) (*s3.Bucket, error) {
	auth, err := aws.GetAuth("", "", "", time.Time{})
	if err != nil {
		return nil, err
	}

	s := s3.New(auth, aws.USWest2)
	s.ConnectTimeout = time.Second * 30
	s.ReadTimeout = time.Second * 30

	bucketName := strings.TrimPrefix(bucketPrefix, "s3://") + "-" + environment.GetCloudEnv()
	return s.Bucket(bucketName), nil
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
