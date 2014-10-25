package gobuddyfs

import (
	"sync"

	"github.com/golang/glog"
)

type MemStore struct {
	lock  *sync.RWMutex
	store map[string][]byte

	KVStore
}

func NewMemStore() *MemStore {
	return &MemStore{store: make(map[string][]byte), lock: &sync.RWMutex{}}
}

func (self *MemStore) Get(key string, retry bool) ([]byte, error) {
	if glog.V(2) {
		glog.Infof("Get(%s)\n", key)
	}
	self.lock.RLock()
	defer self.lock.RUnlock()
	val, ok := self.store[key]

	if !ok {
		return nil, nil
	}

	return val, nil
}

func (self *MemStore) Set(key string, value []byte) error {
	if glog.V(2) {
		glog.Infof("Set(%s)\n", key)
	}
	self.lock.Lock()
	defer self.lock.Unlock()

	if value == nil {
		// Implicit delete operation
		delete(self.store, key)
	} else {
		self.store[key] = value
	}

	return nil
}

var _ KVStore = new(MemStore)
