package gobuddyfs

import (
	"encoding/json"
	"os"
	"sync"
	"syscall"

	"github.com/golang/glog"
	"golang.org/x/net/context"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

// Dir implements both Node and Handle for the root directory.
type Dir struct {
	Dirs   []Block
	Files  []Block
	Lock   sync.RWMutex   `json:"-"`
	store  KVStore        `json:"-"`
	BFS    *BuddyFS       `json:"-"`
	KVS    KVStore        `json:"-"`
	blkGen BlockGenerator `json:"-"`
	Block
	fs.Node
}

var _ Marshalable = new(Dir)

func (dir *Dir) Marshal() ([]byte, error) {
	return json.Marshal(dir)
}

func (dir *Dir) Unmarshal(data []byte) error {
	return json.Unmarshal(data, dir)
}

func (dir *Dir) Forget() {
	if glog.V(2) {
		glog.Infoln("FORGET", dir.Name)
	}
}

func (dir Dir) Attr(attr *fuse.Attr) {
	attr.Mode = os.ModeDir | 0555
	attr.Inode = uint64(dir.Id)
}

func (dir *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	if glog.V(2) {
		glog.Infof("Looking up file %s", name)
	}

	dir.Lock.RLock()
	defer dir.Lock.RUnlock()

	_, _, node, err := dir.LookupUnlocked(ctx, name)
	return node, err
}

func (dir *Dir) LookupUnlocked(ctx context.Context, name string) (bool, int, fs.Node, error) {
	for dirId := range dir.Dirs {
		if dir.Dirs[dirId].Name == name {
			var dirDir Dir
			dirDir.Id = dir.Dirs[dirId].Id

			err := dirDir.ReadBlock(&dirDir, dir.KVS)
			if err != nil {
				glog.Errorf("Error while read dir block: %q", err)
				return true, dirId, nil, fuse.EIO
			}

			dirDir.KVS = dir.KVS
			dirDir.blkGen = dir.blkGen
			return true, dirId, &dirDir, nil
		}
	}

	for fileId := range dir.Files {
		if dir.Files[fileId].Name == name {
			var file File
			file.Block.Id = dir.Files[fileId].Id

			err := file.ReadBlock(&file, dir.KVS)
			if err != nil {
				glog.Errorf("Error while read file block: %q", err)
				return false, fileId, nil, fuse.EIO
			}

			file.KVS = dir.KVS
			file.blkGen = dir.blkGen
			return false, fileId, &file, nil
		}
	}

	return false, 0, nil, fuse.ENOENT
}

func (dir *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	if glog.V(2) {
		glog.Infof("Mkdir %s %d", req.Name, len(req.Name))
	}

	if len(req.Name) > 255 {
		return nil, fuse.Errno(syscall.ENAMETOOLONG)
	}

	dir.Lock.Lock()
	defer dir.Lock.Unlock()

	_, _, _, err := dir.LookupUnlocked(ctx, req.Name)
	if err != fuse.ENOENT {
		return nil, fuse.Errno(syscall.EEXIST)
	}

	blk := dir.blkGen.NewNamedBlock(req.Name)

	newDir := &Dir{Block: blk, KVS: dir.KVS, blkGen: dir.blkGen, Dirs: []Block{},
		Files: []Block{}, Lock: sync.RWMutex{}}
	newDir.MarkDirty()
	err = newDir.WriteBlock(newDir, dir.KVS)
	if err != nil {
		return nil, fuse.EIO
	}

	dir.Dirs = append(dir.Dirs, blk)
	dir.MarkDirty()
	dir.WriteBlock(dir, dir.KVS)
	if err != nil {
		return nil, fuse.EIO
	}

	return newDir, nil
}

func (dir *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	if glog.V(2) {
		glog.Infof("Removing %s %d", req.Name, len(req.Name))
	}

	if len(req.Name) > 255 {
		return fuse.Errno(syscall.ENAMETOOLONG)
	}

	dir.Lock.Lock()
	defer dir.Lock.Unlock()
	isDir, posn, node, err := dir.LookupUnlocked(ctx, req.Name)

	if err != nil {
		return err
	}

	if !isDir {
		dir.Files = append(dir.Files[:posn], dir.Files[posn+1:]...)
		dir.MarkDirty()
		dir.WriteBlock(dir, dir.KVS)
		if err != nil {
			return fuse.EIO
		}

		return nil
	} else {
		dirDir, ok := node.(*Dir)
		if !ok {
			return fuse.EIO
		}

		if len(dirDir.Dirs) != 0 || len(dirDir.Files) != 0 {
			return fuse.Errno(syscall.ENOTEMPTY)
		}

		dir.Dirs = append(dir.Dirs[:posn], dir.Dirs[posn+1:]...)
		dir.MarkDirty()
		dir.WriteBlock(dir, dir.KVS)
		if err != nil {
			return fuse.EIO
		}

		return nil
	}

	return fuse.ENOSYS
}

func (dir *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	if glog.V(2) {
		glog.Infof("Creating file %s %d", req.Name, len(req.Name))
	}

	if len(req.Name) > 255 {
		return nil, nil, fuse.Errno(syscall.ENAMETOOLONG)
	}

	dir.Lock.Lock()
	defer dir.Lock.Unlock()

	_, _, _, err := dir.LookupUnlocked(ctx, req.Name)
	if err != fuse.ENOENT {
		return nil, nil, fuse.Errno(syscall.EEXIST)
	}

	blk := dir.blkGen.NewNamedBlock(req.Name)

	newFile := &File{Block: blk, Blocks: []StorageUnit{}, KVS: dir.KVS, blkGen: dir.blkGen}
	newFile.MarkDirty()
	err = newFile.WriteBlock(newFile, dir.KVS)
	if err != nil {
		return nil, nil, fuse.EIO
	}

	dir.Files = append(dir.Files, blk)
	dir.MarkDirty()
	dir.WriteBlock(dir, dir.KVS)
	if err != nil {
		return nil, nil, fuse.EIO
	}

	return newFile, newFile, nil
}

func (dir *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
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
