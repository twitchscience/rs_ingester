package metadata

type TestBackend struct {
	channel chan *LoadManifest
}

func NewTestBackend() MetadataBackend {
	return &TestBackend{channel: make(chan *LoadManifest, 1)}
}

func (t *TestBackend) LoadReady() chan *LoadManifest {
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

func (t *TestBackend) PingDB() error {
	return nil
}

func (t *TestBackend) Close() {
	close(t.channel)
}
