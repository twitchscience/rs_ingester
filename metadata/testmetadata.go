package metadata

type TestBackend struct {
	channel chan *LoadBatch
}

func NewTestBackend() MetadataBackend {
	return &TestBackend{channel: make(chan *LoadBatch, 1)}
}

func (t *TestBackend) LoadReady() chan *LoadBatch {
	return t.channel
}

func (t *TestBackend) LoadDone(loadUuid string) {
	return
}

func (t *TestBackend) LoadError(loadUuid, loadError string) {
	return
}

func (t *TestBackend) InsertLoad(load *Load) error {
	return nil
}

func (t *TestBackend) Close() {
	close(t.channel)
}
