package gobuddyfs

import (
	"encoding/binary"
	"encoding/json"
	"math/rand"
	"os"
	"strconv"
	"sync"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/golang/glog"
)

// BuddyFS implements the Buddy file system.
type BuddyFS struct {
	Lock  sync.Mutex
	Store KVStore
	FSM   *FSMeta

	fs.FS
}

type FSMeta struct {
	NextInode int
	Dir
}

func NewBuddyFS(store KVStore) *BuddyFS {
	bfs := &BuddyFS{Store: store, Lock: sync.Mutex{}}
	return bfs
}

func (bfs BuddyFS) CreateNewFSMetadata() *FSMeta {
	return &FSMeta{NextInode: 2,
		Dir: Dir{Block: Block{name: "/", Inode: 1, Id: rand.Int63()}}}
}

func (bfs *BuddyFS) Root() (fs.Node, fuse.Error) {
	bfs.Lock.Lock()
	defer bfs.Lock.Unlock()

	if bfs.FSM == nil {
		rootKey, err := bfs.Store.Get("ROOT")

		if err != nil {
			// Error reading the key
			return nil, fuse.ENODATA
		} else if rootKey == nil {
			// Root key not found
			root := bfs.CreateNewFSMetadata()
			err = root.Write(bfs.Store)
			if err == nil {
				buffer := make([]byte, 80)
				binary.PutVarint(buffer, root.Block.Id)
				err = bfs.Store.Set("ROOT", buffer)
				if err == nil {
					bfs.FSM = root
					return bfs.FSM, nil
				} else {
					glog.Errorf("Error while creating ROOT key: %q", err)
					return nil, fuse.ENODATA
				}
			} else {
				glog.Errorf("Error while creating root node: %q", err)
				return nil, fuse.ENODATA
			}
		}

		var root FSMeta
		var n int
		root.Block.Id, n = binary.Varint(rootKey)
		if n <= 0 {
			glog.Errorf("Error while decoding root key")
			return nil, fuse.ENODATA
		}

		err = root.Read(bfs.Store)
		if err != nil {
			glog.Errorf("Error while read root block: %q", err)
			return nil, fuse.ENODATA
		}

		bfs.FSM = &root
		return bfs.FSM, nil
	}

	return bfs.FSM, nil
}

type Block struct {
	name string
	// TODO: Can inode number be used as Id?
	Id    int64
	Inode uint64
}

func (b *Block) Write(store KVStore) error {
	bEncoded, err := json.Marshal(b)
	if err != nil {
		return err
	}

	return store.Set(strconv.FormatInt(b.Id, 10), bEncoded)
}

func (b *Block) Read(store KVStore) error {
	encoded, err := store.Get(strconv.FormatInt(b.Id, 10))
	if err != nil {
		return err
	}

	err = json.Unmarshal(encoded, b)

	if err != nil {
		return err
	}

	return nil
}

// Dir implements both Node and Handle for the root directory.
type Dir struct {
	dirs  []Block
	files []Block
	store KVStore `json:"-"`
	Block
}

// This method should be related to FS
// so that the appropriate inode ID can be set.
func NewDir(name string) *Dir {
	// FIXME: Change inode 1 below
	return &Dir{Block: Block{name: name, Inode: 1, Id: rand.Int63()}}
}

func (dir Dir) Attr() fuse.Attr {
	return fuse.Attr{Inode: dir.Inode, Mode: os.ModeDir | 0555}
}

func (dir Dir) Lookup(name string, intr fs.Intr) (fs.Node, fuse.Error) {
	for dirId := range dir.dirs {
		if dir.dirs[dirId].name == name {
			var dirDir Dir
			dirDir.Block.Id = dir.dirs[dirId].Id

			err := dirDir.Read(dir.store)
			if err != nil {
				glog.Errorf("Error while read dir block: %q", err)
				return nil, fuse.ENODATA
			}

			return dirDir, nil
		}
	}

	for fileId := range dir.files {
		if dir.files[fileId].name == name {
			var file File
			file.Block.Id = dir.files[fileId].Id

			err := file.Read(dir.store)
			if err != nil {
				glog.Errorf("Error while read dir block: %q", err)
				return nil, fuse.ENODATA
			}

			return file, nil
		}
	}

	return nil, fuse.ENOENT
}

func (dir Dir) ReadDir(intr fs.Intr) ([]fuse.Dirent, fuse.Error) {
	dirEnts := []fuse.Dirent{}

	for dirId := range dir.dirs {
		dirDir := fuse.Dirent{Inode: dir.dirs[dirId].Inode, Name: dir.dirs[dirId].name, Type: fuse.DT_Dir}
		dirEnts = append(dirEnts, dirDir)
	}

	for fileId := range dir.files {
		dirFile := fuse.Dirent{Inode: dir.files[fileId].Inode, Name: dir.files[fileId].name, Type: fuse.DT_File}
		dirEnts = append(dirEnts, dirFile)
	}

	return dirEnts, nil
}

// File implements both Node and Handle for the hello file.
type File struct {
	Block
}

func (File) Attr() fuse.Attr {
	return fuse.Attr{Inode: 2, Mode: 0444}
}

func (File) ReadAll(intr fs.Intr) ([]byte, fuse.Error) {
	return []byte("hello, world\n"), nil
}
