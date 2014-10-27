package gobuddyfs

import (
	"sync"

	"github.com/golang/glog"
	"github.com/steveyen/gkvlite"
)

type GKVStore struct {
	collection *gkvlite.Collection
	store      *gkvlite.Store
	lock       sync.RWMutex

	// Implements: KVStore
}

func NewGKVStore(collection *gkvlite.Collection, store *gkvlite.Store) *GKVStore {
	return &GKVStore{collection: collection, store: store}
}

func (self *GKVStore) Get(key string, retry bool) ([]byte, error) {
	defer self.lock.RUnlock()
	self.lock.RLock()

	if glog.V(2) {
		glog.Infof("Get(%s)\n", key)
	}
	return self.collection.Get([]byte(key))
}

func (self *GKVStore) Set(key string, value []byte) error {
	defer self.lock.Unlock()
	self.lock.Lock()

	if glog.V(2) {
		glog.Infof("Set(%s)\n", key)
	}
	err := self.collection.Set([]byte(key), value)
	self.collection.Write()
	self.store.Flush()
	return err
}

var _ KVStore = new(GKVStore)
