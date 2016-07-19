package blueprint

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"

	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

// Client is an client for the http interface of blueprint
type Client struct {
	host string
}

// New returns a new Blueprint Client
func New(host string) Client {
	return Client{host: host}
}

// GetMigration hits blueprint's migration endpoint for finding how to migrate
// to `toVersion` for table `table`
func (c *Client) GetMigration(table string, toVersion int) ([]scoop_protocol.Operation, error) {
	v := url.Values{}
	v.Set("to_version", strconv.Itoa(toVersion))
	u := url.URL{
		Scheme:   "http",
		Host:     c.host,
		Path:     fmt.Sprintf("migration/%s", table),
		RawQuery: v.Encode(),
	}
	resp, err := http.Get(u.String())
	defer func() {
		err = resp.Body.Close()
		if err != nil {
			logger.WithError(err).Error("Error closing response body from blueprint")
		}
	}()
	if err != nil {
		return nil, fmt.Errorf("Error GETing migration from blueprint: %v", err)
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("Received %v from blueprint when GETing migration at %s", resp.Status, u.String())
	}
	var ops []scoop_protocol.Operation
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("Error reading migration body from blueprint: %v", err)
	}
	err = json.Unmarshal(body, &ops)
	if err != nil {
		return nil, fmt.Errorf("Error parsing json migration response from bluprint at %s: %v", u.String(), err)
	}
	return ops, nil
}
