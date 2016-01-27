package loadclient

import (
	"encoding/json"
	"testing"

	"github.com/twitchscience/rs_ingester/metadata"
)

func TestMakeManifestFile(t *testing.T) {
	b := metadata.LoadManifest{
		Loads: []metadata.Load{
			metadata.Load{KeyName: "foo", TableName: "bar"},
			metadata.Load{KeyName: "fiz", TableName: "bar"},
			metadata.Load{KeyName: "buz", TableName: "bar"},
		},
		TableName: "bar",
		UUID:      "deadbeef",
	}

	buf, err := makeManifestJson(&b)
	if err != nil {
		t.Fatal("Got error making manifest", err)
	}

	var m manifest
	if err := json.Unmarshal(buf, &m); err != nil {
		t.Fatal("Got error decoding manifest", err)
	}

	if len(m.Entries) != 3 {
		t.Fatal("Unexpected number of entries in the manifest file!")
	}

	if m.Entries[0].URL != "s3://foo" {
		t.Fatal("Unexpected URL for first load in manifest")
	}
}
