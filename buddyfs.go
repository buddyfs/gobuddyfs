package gobuddyfs

import (
	"encoding/binary"
	"encoding/json"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"syscall"

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
	NextInode uint64
	Store     *KVStore `json:"-"`
	Dir
}

func (fsm FSMeta) Marshal() ([]byte, error) {
	return json.Marshal(fsm)
}

func NewBuddyFS(store KVStore) *BuddyFS {
	bfs := &BuddyFS{Store: store, Lock: sync.Mutex{}}
	return bfs
}

func (bfs BuddyFS) CreateNewFSMetadata() *FSMeta {
	return &FSMeta{NextInode: 2,
		Dir: Dir{Block: Block{Name: "/", Inode: 1, Id: rand.Int63()},
			Dirs: []Block{}, Files: []Block{}, Lock: sync.RWMutex{}}}
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
			err = root.Write(root, bfs.Store)
			if err == nil {
				buffer := make([]byte, 80)
				binary.PutVarint(buffer, root.Block.Id)
				err = bfs.Store.Set("ROOT", buffer)
				if err == nil {
					bfs.FSM = root
					bfs.FSM.Root = bfs.FSM
					bfs.FSM.Store = &bfs.Store
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
		bfs.FSM.Root = bfs.FSM
		bfs.FSM.Store = &bfs.Store
		return bfs.FSM, nil
	}

	return bfs.FSM, nil
}

type Marshalable interface {
	Marshal() ([]byte, error)
}

type Block struct {
	Name string
	// TODO: Can inode number be used as Id?
	Id    int64
	Inode uint64
}

func (b Block) Write(m Marshalable, store KVStore) error {
	bEncoded, err := m.Marshal()
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
	Dirs  []Block
	Files []Block
	Lock  sync.RWMutex
	store KVStore `json:"-"`
	Root  *FSMeta `json:"-"`
	Block
}

func (dir Dir) Marshal() ([]byte, error) {
	return json.Marshal(dir)
}

func (dir Dir) Attr() fuse.Attr {
	return fuse.Attr{Inode: dir.Inode, Mode: os.ModeDir | 0555}
}

func (dir Dir) Lookup(name string, intr fs.Intr) (fs.Node, fuse.Error) {
	dir.Lock.RLock()
	defer dir.Lock.RUnlock()

	return dir.LookupUnlocked(name, intr)
}

func (dir Dir) LookupUnlocked(name string, intr fs.Intr) (fs.Node, fuse.Error) {
	for dirId := range dir.Dirs {
		if dir.Dirs[dirId].Name == name {
			var dirDir Dir
			dirDir.Id = dir.Dirs[dirId].Id

			err := dirDir.Read(*dir.Root.Store)
			if err != nil {
				glog.Errorf("Error while read dir block: %q", err)
				return nil, fuse.ENODATA
			}

			return dirDir, nil
		}
	}

	for fileId := range dir.Files {
		if dir.Files[fileId].Name == name {
			var file File
			file.Block.Id = dir.Files[fileId].Id

			err := file.Read(*dir.Root.Store)
			if err != nil {
				glog.Errorf("Error while read dir block: %q", err)
				return nil, fuse.ENODATA
			}

			return file, nil
		}
	}

	return nil, fuse.ENOENT
}

func (dir *Dir) Mkdir(req *fuse.MkdirRequest, intr fs.Intr) (fs.Node, fuse.Error) {
	dir.Lock.Lock()
	defer dir.Lock.Unlock()

	_, err := dir.LookupUnlocked(req.Name, intr)
	if err != fuse.ENOENT {
		return nil, fuse.Errno(syscall.EEXIST)
	}

	blk := Block{Name: req.Name, Inode: dir.Root.NextInode, Id: rand.Int63()}

	dir.Root.NextInode++
	err = dir.Root.Write(dir.Root, *dir.Root.Store)
	if err != nil {
		return nil, fuse.ENODATA
	}

	newDir := &Dir{Block: blk, Root: dir.Root, Dirs: []Block{}, Files: []Block{}, Lock: sync.RWMutex{}}
	err = newDir.Write(newDir, *dir.Root.Store)
	if err != nil {
		return nil, fuse.ENODATA
	}

	dir.Dirs = append(dir.Dirs, blk)
	dir.Write(dir, *dir.Root.Store)
	if err != nil {
		return nil, fuse.ENODATA
	}

	return newDir, nil
}

func (dir Dir) ReadDir(intr fs.Intr) ([]fuse.Dirent, fuse.Error) {
	dirEnts := []fuse.Dirent{}

	for dirId := range dir.Dirs {
		dirDir := fuse.Dirent{Inode: dir.Dirs[dirId].Inode, Name: dir.Dirs[dirId].Name, Type: fuse.DT_Dir}
		dirEnts = append(dirEnts, dirDir)
	}

	for fileId := range dir.Files {
		dirFile := fuse.Dirent{Inode: dir.Files[fileId].Inode, Name: dir.Files[fileId].Name, Type: fuse.DT_File}
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
