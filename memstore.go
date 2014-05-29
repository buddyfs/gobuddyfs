package gobuddyfs

import (
	"sync"

	"github.com/golang/glog"

	// "github.com/golang/glog"
)

type MemStore struct {
	writeLock *sync.Mutex
	store     map[string][]byte

	KVStore
}

func NewMemStore() *MemStore {
	return &MemStore{store: make(map[string][]byte), writeLock: &sync.Mutex{}}
}

func (self *MemStore) Get(key string) ([]byte, error) {
	glog.Infof("Get(%s)\n", key)
	val, ok := self.store[key]

	if !ok {
		return nil, nil
	}

	return val, nil
}

func (self *MemStore) Set(key string, value []byte) error {
	// glog.Infof("Set(%s, %s)\n", key, value)
	self.writeLock.Lock()
	defer self.writeLock.Unlock()

	self.store[key] = value

	return nil
}

var _ KVStore = new(MemStore)
