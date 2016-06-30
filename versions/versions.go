package versions

import "sync"

// Getter is an interface for reading table versions
type Getter interface {
	Get(string) (int, bool)
}

// Setter is an interface for writing table versions
type Setter interface {
	Set(string, int)
}

// GetterSetter is an interface for both reading and writing table versions
type GetterSetter interface {
	Getter
	Setter
}

// New returns a new GetterSetter versions map from a given map
func New(init map[string]int) GetterSetter {
	return versions{
		content: init,
		mutex:   &sync.RWMutex{},
	}
}

type versions struct {
	mutex   *sync.RWMutex
	content map[string]int
}

func (v versions) Get(table string) (int, bool) {
	v.mutex.RLock()
	defer v.mutex.RUnlock()

	val, ok := v.content[table]
	return val, ok
}

func (v versions) Set(table string, val int) {
	v.mutex.Lock()
	defer v.mutex.Unlock()

	v.content[table] = val
}
