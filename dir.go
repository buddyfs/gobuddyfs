package gobuddyfs

import (
	"encoding/json"
	"math/rand"
	"os"
	"sync"
	"syscall"

	"github.com/golang/glog"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

// Dir implements both Node and Handle for the root directory.
type Dir struct {
	Dirs  []Block
	Files []Block
	Lock  sync.RWMutex
	store KVStore `json:"-"`
	Root  *FSMeta `json:"-"`
	Block
	fs.Node
}

func (dir *Dir) Marshal() ([]byte, error) {
	return json.Marshal(dir)
}

func (dir *Dir) Forget() {
	if glog.V(2) {
		glog.Infoln("FORGET", dir.Name)
	}
}

func (dir Dir) Attr() fuse.Attr {
	return fuse.Attr{Mode: os.ModeDir | 0555}
}

func (dir *Dir) Lookup(name string, intr fs.Intr) (fs.Node, fuse.Error) {
	dir.Lock.RLock()
	defer dir.Lock.RUnlock()

	return dir.LookupUnlocked(name, intr)
}

func (dir *Dir) LookupUnlocked(name string, intr fs.Intr) (fs.Node, fuse.Error) {
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
	if glog.V(2) {
		glog.Infof("Creating file %s\n", req.Name)
	}
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

func (dir *Dir) ReadDir(intr fs.Intr) ([]fuse.Dirent, fuse.Error) {
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
