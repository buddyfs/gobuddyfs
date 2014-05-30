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

const BLOCK_SIZE = 65536

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

// BuddyFS implements the Buddy file system.
type BuddyFS struct {
	Lock  sync.Mutex
	Store KVStore
	FSM   *FSMeta

	fs.FS
}

type FSMeta struct {
	Store *KVStore `json:"-"`
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
	return &FSMeta{Dir: Dir{Block: Block{Name: "/", Id: rand.Int63()},
		Dirs: []Block{}, Files: []Block{}, Lock: sync.RWMutex{}}}
}

func (bfs *BuddyFS) Root() (fs.Node, fuse.Error) {
	bfs.Lock.Lock()
	defer bfs.Lock.Unlock()

	if bfs.FSM == nil {
		rootKey, err := bfs.Store.Get("ROOT")

		if err != nil {
			// Error reading the key
			return nil, fuse.EIO
		} else if rootKey == nil {
			// Root key not found
			root := bfs.CreateNewFSMetadata()
			err = root.WriteBlock(root, bfs.Store)
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
	Id int64
}

type DataBlock struct {
	Block
	Data []byte
}

func (dBlock DataBlock) Marshal() ([]byte, error) {
	return json.Marshal(dBlock)
}

func (b Block) WriteBlock(m Marshalable, store KVStore) error {
	bEncoded, err := m.Marshal()
	if err != nil {
		return err
	}

	return store.Set(strconv.FormatInt(b.Id, 10), bEncoded)
}

func (b *Block) ReadBlock(m interface{}, store KVStore) error {
	encoded, err := store.Get(strconv.FormatInt(b.Id, 10))
	if err != nil {
		return err
	}

	err = json.Unmarshal(encoded, m)

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

func (dir Dir) Forget() {
	glog.Infoln("FORGET", dir.Name)
}

func (dir Dir) Attr() fuse.Attr {
	return fuse.Attr{Mode: os.ModeDir | 0555}
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

			err := dirDir.ReadBlock(&dirDir, *dir.Root.Store)
			if err != nil {
				glog.Errorf("Error while read dir block: %q", err)
				return nil, fuse.EIO
			}

			return dirDir, nil
		}
	}

	for fileId := range dir.Files {
		if dir.Files[fileId].Name == name {
			var file File
			file.Block.Id = dir.Files[fileId].Id

			err := file.ReadBlock(&file, *dir.Root.Store)
			if err != nil {
				glog.Errorf("Error while read dir block: %q", err)
				return nil, fuse.EIO
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

	blk := Block{Name: req.Name, Id: rand.Int63()}

	newDir := &Dir{Block: blk, Root: dir.Root, Dirs: []Block{}, Files: []Block{}, Lock: sync.RWMutex{}}
	err = newDir.WriteBlock(newDir, *dir.Root.Store)
	if err != nil {
		return nil, fuse.EIO
	}

	dir.Dirs = append(dir.Dirs, blk)
	dir.WriteBlock(dir, *dir.Root.Store)
	if err != nil {
		return nil, fuse.EIO
	}

	return newDir, nil
}

func (dir *Dir) Create(req *fuse.CreateRequest, resp *fuse.CreateResponse, intr fs.Intr) (fs.Node, fs.Handle, fuse.Error) {
	glog.Infof("Creating file %s\n", req.Name)
	dir.Lock.Lock()
	defer dir.Lock.Unlock()

	_, err := dir.LookupUnlocked(req.Name, intr)
	if err != fuse.ENOENT {
		return nil, nil, fuse.Errno(syscall.EEXIST)
	}

	blk := Block{Name: req.Name, Id: rand.Int63()}

	newFile := &File{Block: blk, Blocks: []Block{}, Root: dir.Root}
	err = newFile.WriteBlock(newFile, *dir.Root.Store)
	if err != nil {
		return nil, nil, fuse.EIO
	}

	dir.Files = append(dir.Files, blk)
	dir.WriteBlock(dir, *dir.Root.Store)
	if err != nil {
		return nil, nil, fuse.EIO
	}

	return newFile, newFile, nil
}

func (dir Dir) ReadDir(intr fs.Intr) ([]fuse.Dirent, fuse.Error) {
	dirEnts := []fuse.Dirent{}

	for dirId := range dir.Dirs {
		dirDir := fuse.Dirent{Name: dir.Dirs[dirId].Name, Type: fuse.DT_Dir}
		dirEnts = append(dirEnts, dirDir)
	}

	for fileId := range dir.Files {
		dirFile := fuse.Dirent{Name: dir.Files[fileId].Name, Type: fuse.DT_File}
		dirEnts = append(dirEnts, dirFile)
	}

	return dirEnts, nil
}

// File implements both Node and Handle for the hello file.
type File struct {
	Block
	Size   uint64
	Blocks []Block
	Root   *FSMeta `json:"-"`
}

func (file File) Open(req *fuse.OpenRequest, res *fuse.OpenResponse, intr fs.Intr) (fs.Handle, fuse.Error) {
	glog.Infoln("Open called")
	return file, nil
}

func (file File) Setattr(req *fuse.SetattrRequest, res *fuse.SetattrResponse, intr fs.Intr) fuse.Error {
	glog.Infoln("Setattr called")
	glog.Infoln("Req: ", req)
	if req.Size != 0 {
		// Resizing!
		file.Size = req.Size
		glog.Infoln("Resizing file to size", file.Size)
		res.Attr = file.Attr()
		return nil
	}

	res.Attr = file.Attr()
	glog.Infoln("Finished Setattr")
	// TODO: Not implemented.
	return nil
}

func (file *File) Write(req *fuse.WriteRequest, res *fuse.WriteResponse, intr fs.Intr) fuse.Error {
	dataBytes := len(req.Data)
	glog.Infof("Writing %d byte(s)", dataBytes)
	for req.Offset+int64(dataBytes) >= int64(BLOCK_SIZE*len(file.Blocks)) {
		blk := Block{Id: rand.Int63()}
		dBlk := DataBlock{Block: blk, Data: []byte{}}
		err := dBlk.WriteBlock(dBlk, *file.Root.Store)
		if err != nil {
			return fuse.EIO
		}

		file.Blocks = append(file.Blocks, blk)
		err = file.WriteBlock(file, *file.Root.Store)
		if err != nil {
			return fuse.EIO
		}
	}

	startBlockId := req.Offset / BLOCK_SIZE
	startBlockLoc := file.Blocks[startBlockId]

	var startBlock DataBlock
	startBlock.Block.Id = startBlockLoc.Id

	err := startBlock.ReadBlock(&startBlock, *file.Root.Store)
	if err != nil {
		glog.Errorf("Error while read root block: %q", err)
		return fuse.EIO
	}

	glog.Infof("Block content length: %d", len(startBlock.Data))
	bytesToAdd := min(BLOCK_SIZE-len(startBlock.Data), dataBytes)
	startBlock.Data = append(startBlock.Data, req.Data[0:bytesToAdd]...)
	glog.Infof("Block content length after: %d", len(startBlock.Data))
	err = startBlock.WriteBlock(startBlock, *file.Root.Store)
	if err != nil {
		glog.Error(err)
		return fuse.EIO
	}

	glog.Infoln("Successfully completed write operation")
	res.Size = bytesToAdd
	file.Size += uint64(bytesToAdd)

	err = file.WriteBlock(file, *file.Root.Store)
	if err != nil {
		return fuse.EIO
	}
	return nil
}

func (file File) Marshal() ([]byte, error) {
	return json.Marshal(file)
}

func (file File) Attr() fuse.Attr {
	glog.Infoln("Attr called")
	return fuse.Attr{Mode: 0444, Blocks: uint64(len(file.Blocks)), Size: file.Size}
}

func (file File) Release(req *fuse.ReleaseRequest, intr fs.Intr) fuse.Error {
	glog.Infoln("Release", file.Name)
	return nil
}

func (file File) Forget() {
	glog.Infoln("FORGET", file.Name)
}

func (file File) Read(req *fuse.ReadRequest, res *fuse.ReadResponse, intr fs.Intr) fuse.Error {
	glog.Infof("Reading %d byte(s) at offset %d", req.Size, req.Offset)

	if req.Offset > int64(file.Size) {
		res.Data = []byte{}
		return nil
	}

	res.Data = []byte{}

	startBlockId := req.Offset / BLOCK_SIZE
	startBlockLoc := file.Blocks[startBlockId]

	var startBlock DataBlock
	startBlock.Block.Id = startBlockLoc.Id

	err := startBlock.ReadBlock(&startBlock, *file.Root.Store)
	if err != nil {
		glog.Errorf("Error while reading block: %q", err)
		return fuse.EIO
	}

	beginReadByte := int(req.Offset % BLOCK_SIZE)
	endReadByte := min(len(startBlock.Data)-beginReadByte, req.Size)
	glog.Infof("Block content length: %d", len(startBlock.Data))

	glog.Infof("Reading from %d to %d in block %d", beginReadByte, endReadByte+beginReadByte, startBlockId)
	res.Data = startBlock.Data[beginReadByte : endReadByte+beginReadByte]

	return nil
}
