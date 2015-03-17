package gobuddyfs

import (
	"encoding/binary"
	"encoding/json"
	"sync"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/golang/glog"
)

const BLOCK_SIZE = 4096
const ROOT_BLOCK_KEY = "ROOT"

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

// BuddyFS implements the Buddy file system.
type BuddyFS struct {
	Lock   sync.Mutex
	Store  KVStore
	blkGen BlockGenerator
	FSM    *FSMeta

	fs.FS
}

type FSMeta struct {
	Dir
}

func (fsm FSMeta) Marshal() ([]byte, error) {
	return json.Marshal(fsm)
}

func (fsm *FSMeta) Unmarshal(data []byte) error {
	return json.Unmarshal(data, fsm)
}

func NewBuddyFS(store KVStore) *BuddyFS {
	bfs := &BuddyFS{Store: store, Lock: sync.Mutex{},
		blkGen: new(RandomizedBlockGenerator)}
	return bfs
}

func (bfs BuddyFS) CreateNewFSMetadata() *FSMeta {
	return &FSMeta{Dir: Dir{Block: bfs.blkGen.NewNamedBlock("/"),
		blkGen: bfs.blkGen, Dirs: []Block{}, Files: []Block{}, Lock: sync.RWMutex{}}}
}

func (bfs *BuddyFS) Root() (fs.Node, error) {
	bfs.Lock.Lock()
	defer bfs.Lock.Unlock()

	if bfs.FSM == nil {
		rootKey, err := bfs.Store.Get(ROOT_BLOCK_KEY, true)

		if rootKey == nil {
			glog.Infoln("Creating new root block")
			// Root key not found
			root := bfs.CreateNewFSMetadata()
			root.MarkDirty()
			err = root.WriteBlock(root, bfs.Store)
			if err == nil {
				buffer := make([]byte, 80)
				binary.PutVarint(buffer, root.Block.Id)
				err = bfs.Store.Set(ROOT_BLOCK_KEY, buffer)
				if err == nil {
					bfs.FSM = root
					bfs.FSM.KVS = bfs.Store
					return bfs.FSM, nil
				} else {
					glog.Errorf("Error while creating ROOT key: %q", err)
					return nil, fuse.EIO
				}
			} else {
				glog.Errorf("Error while creating root node: %q", err)
				return nil, fuse.EIO
			}
		}

		var root FSMeta
		var n int
		root.Block.Id, n = binary.Varint(rootKey)
		if n <= 0 {
			glog.Errorf("Error while decoding root key")
			return nil, fuse.EIO
		}

		err = root.ReadBlock(&root, bfs.Store)
		if err != nil {
			glog.Errorf("Error while read root block: %q", err)
			return nil, fuse.EIO
		}

		bfs.FSM = &root
		bfs.FSM.KVS = bfs.Store
		bfs.FSM.blkGen = new(RandomizedBlockGenerator)
		return bfs.FSM, nil
	}

	return bfs.FSM, nil
}
