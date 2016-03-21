package loadclient

import (
	"strings"
	"time"

	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/s3"
	"github.com/twitchscience/aws_utils/environment"
)

type loadError struct {
	msg         string
	isRetryable bool
}

func (e loadError) Error() string {
	return e.msg
}

func (e loadError) Retryable() bool {
	return e.isRetryable
}

type entry struct {
	URL       string `json:"url"`
	Mandatory bool   `json:"mandatory"`
}

type manifest struct {
	Entries []entry `json:"entries"`
}

//GetBucket makes a bucket in s3 with a path specified through instance variables
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
