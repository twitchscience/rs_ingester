package scoop

import (
	"fmt"
	"log"
	"net/http"
)

// Client is a client to interface with scoop's HTTP api
type Client struct {
	baseURL string
}

// New returns a new scoop client
func New(baseURL string) Client {
	return Client{baseURL: baseURL}
}

// EnforcePermissions hits scoop's endpoint to enforce db permissions on ace
// tables
func (c *Client) EnforcePermissions() error {
	resp, err := http.Post(c.baseURL+"/db/enforce_perms", "", nil)
	if err != nil {
		return fmt.Errorf("Error enforicng ace db permissions through scoop: %v", err)
	}
	defer func() {
		err = resp.Body.Close()
		if err != nil {
			log.Printf("Error closing scoop enforce permissions response body: %v", err)
		}
	}()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("Received %v from scoop when enforcing ace db permissions", resp.Status)
	}
	return nil
}
