package metadata

import "github.com/twitchscience/scoop_protocol/scoop_protocol"

type Load scoop_protocol.RowCopyRequest

type LoadBatch struct {
	Loads     []Load
	TableName string
	UUID      string
}

type MetadataBackend interface {
	MetadataStorer
	LoadReady() chan *LoadBatch
	LoadError(batchUuid, loadError string)
	LoadDone(batchUuid string)
}

type MetadataStorer interface {
	InsertLoad(load *Load) error
	Close()
}
