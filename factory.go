package gomessagebroker

import (
	"fmt"
	"sync"
)

type AdapterFactory func(config map[string]interface{}) (Broker, error)

var (
	registryMu sync.RWMutex
	registry   = make(map[string]AdapterFactory)
)

func Register(name string, factory AdapterFactory) {
	registryMu.Lock()
	defer registryMu.Unlock()
	if factory == nil {
		panic("gomessagebroker: Register adapter factory is nil")
	}
	if _, dup := registry[name]; dup {
		panic("gomessagebroker: Register called twice for adapter " + name)
	}
	registry[name] = factory
}

func New(name string, config map[string]interface{}) (Broker, error) {
	registryMu.RLock()
	factory, ok := registry[name]
	registryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("gomessagebroker: unknown adapter %q (forgotten import?)", name)
	}
	return factory(config)
}
