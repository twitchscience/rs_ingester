package metadata

import "github.com/twitchscience/scoop_protocol/scoop_protocol"

type Load scoop_protocol.RowCopyRequest

type LoadManifest struct {
	Loads     []Load
	TableName string
	UUID      string
}

type MetadataBackend interface {
	MetadataStorer
	LoadReady() chan *LoadManifest
	LoadError(manifestUuid, loadError string)
	LoadDone(manifestUuid string)
	PingDB() error
}

type MetadataStorer interface {
	InsertLoad(load *Load) error
	Close()
}
