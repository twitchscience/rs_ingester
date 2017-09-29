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

func (c *Client) queryBlueprint(path string, values url.Values, allow404 bool) ([]byte, error) {
	u := url.URL{
		Scheme:   "http",
		Host:     c.host,
		Path:     path,
		RawQuery: values.Encode(),
	}
	resp, err := http.Get(u.String())
	defer func() {
		if resp != nil {
			if err = resp.Body.Close(); err != nil {
				logger.WithError(err).Error("Error closing response body from blueprint")
			}
		}
	}()
	if err != nil {
		return nil, fmt.Errorf("GETing %s from blueprint: %v", path, err)
	}
	if resp.StatusCode >= 400 {
		if allow404 && resp.StatusCode == 404 {
			return nil, nil
		}
		return nil, fmt.Errorf("received %v from blueprint when GETing at %s", resp.Status, u.String())
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading body from %s on blueprint: %v", path, err)
	}
	return body, nil
}

type bpSchema struct {
	Columns []scoop_protocol.ColumnDefinition
}

// GetMigration hits blueprint's migration endpoint for finding how to migrate
// to `toVersion` for table `table`
func (c *Client) GetMigration(table string, toVersion int) (
	[]scoop_protocol.Operation, []scoop_protocol.ColumnDefinition, error) {
	v := url.Values{}
	v.Set("to_version", strconv.Itoa(toVersion))
	body, err := c.queryBlueprint(fmt.Sprintf("migration/%s", table), v, false)
	if err != nil {
		return nil, nil, fmt.Errorf("querying migration for %s version %d: %v", table, toVersion, err)
	}
	var ops []scoop_protocol.Operation
	err = json.Unmarshal(body, &ops)
	if err != nil {
		return nil, nil, fmt.Errorf("parsing migration response for %s version %d: %v", table, toVersion, err)
	}
	v = url.Values{}
	v.Set("version", strconv.Itoa(toVersion))
	body, err = c.queryBlueprint(fmt.Sprintf("schema/%s", table), v, true)
	if err != nil {
		return nil, nil, fmt.Errorf("querying schema for %s version %d: %v", table, toVersion, err)
	}
	// We 404'd because the schema didn't exist (it was dropped and is now being recreated).
	if body == nil {
		return ops, nil, nil
	}
	var schemas []bpSchema
	err = json.Unmarshal(body, &schemas)
	if err != nil {
		return nil, nil, fmt.Errorf("parsing schema response for %s version %d: %v", table, toVersion, err)
	}
	if len(schemas) != 1 {
		return nil, nil, fmt.Errorf("expected exactly one schema when getting %s version %d", table, toVersion)
	}
	return ops, schemas[0].Columns, nil
}
