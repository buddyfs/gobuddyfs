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
	Lock  sync.RWMutex `json:"-"`
	store KVStore      `json:"-"`
	Root  *FSMeta      `json:"-"`
	BFS   *BuddyFS     `json:"-"`
	Block
	fs.Node
}

func (dir *Dir) SafeRoot() *FSMeta {
	if dir.Root != nil {
		return dir.Root
	}
	rt, _ := dir.BFS.Root()

	dir.Root, _ = rt.(*FSMeta)
	return dir.Root
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
	return fuse.Attr{Mode: os.ModeDir | 0555, Inode: uint64(dir.Id)}
}

func (dir *Dir) Lookup(name string, intr fs.Intr) (fs.Node, fuse.Error) {
	dir.Lock.RLock()
	defer dir.Lock.RUnlock()

	_, _, node, err := dir.LookupUnlocked(name, intr)
	return node, err
}

func (dir *Dir) LookupUnlocked(name string, intr fs.Intr) (bool, int, fs.Node, fuse.Error) {
	for dirId := range dir.Dirs {
		if dir.Dirs[dirId].Name == name {
			var dirDir Dir
			dirDir.Id = dir.Dirs[dirId].Id

			err := dirDir.ReadBlock(&dirDir, *dir.SafeRoot().Store)
			if err != nil {
				glog.Errorf("Error while read dir block: %q", err)
				return true, dirId, nil, fuse.EIO
			}

			dirDir.Root = dir.SafeRoot()
			return true, dirId, &dirDir, nil
		}
	}

	for fileId := range dir.Files {
		if dir.Files[fileId].Name == name {
			var file File
			file.Block.Id = dir.Files[fileId].Id

			err := file.ReadBlock(&file, *dir.SafeRoot().Store)
			if err != nil {
				glog.Errorf("Error while read file block: %q", err)
				return false, fileId, nil, fuse.EIO
			}

			file.Root = dir.SafeRoot()
			return false, fileId, &file, nil
		}
	}

	return false, 0, nil, fuse.ENOENT
}

func (dir *Dir) Mkdir(req *fuse.MkdirRequest, intr fs.Intr) (fs.Node, fuse.Error) {
	if glog.V(2) {
		glog.Infof("Mkdir %s %d", req.Name, len(req.Name))
	}

	if len(req.Name) > 255 {
		return nil, fuse.Errno(syscall.ENAMETOOLONG)
	}

	dir.Lock.Lock()
	defer dir.Lock.Unlock()

	_, _, _, err := dir.LookupUnlocked(req.Name, intr)
	if err != fuse.ENOENT {
		return nil, fuse.Errno(syscall.EEXIST)
	}

	blk := Block{Name: req.Name, Id: rand.Int63()}

	newDir := &Dir{Block: blk, Root: dir.SafeRoot(), Dirs: []Block{}, Files: []Block{}, Lock: sync.RWMutex{}}
	newDir.MarkDirty()
	err = newDir.WriteBlock(newDir, *dir.SafeRoot().Store)
	if err != nil {
		return nil, fuse.EIO
	}

	dir.Dirs = append(dir.Dirs, blk)
	dir.MarkDirty()
	dir.WriteBlock(dir, *dir.SafeRoot().Store)
	if err != nil {
		return nil, fuse.EIO
	}

	return newDir, nil
}

func (dir *Dir) Remove(req *fuse.RemoveRequest, intr fs.Intr) fuse.Error {
	if glog.V(2) {
		glog.Infof("Removing %s %d", req.Name, len(req.Name))
	}

	if len(req.Name) > 255 {
		return fuse.Errno(syscall.ENAMETOOLONG)
	}

	dir.Lock.Lock()
	defer dir.Lock.Unlock()
	isDir, posn, node, err := dir.LookupUnlocked(req.Name, intr)

	if err != nil {
		return err
	}

	if !isDir {
		dir.Files = append(dir.Files[:posn], dir.Files[posn+1:]...)
		dir.MarkDirty()
		dir.WriteBlock(dir, *dir.SafeRoot().Store)
		if err != nil {
			return fuse.EIO
		}

		return nil
	} else {
		dirDir, ok := node.(Dir)
		if !ok {
			return fuse.EIO
		}

		if len(dirDir.Dirs) != 0 || len(dirDir.Files) != 0 {
			return fuse.Errno(syscall.ENOTEMPTY)
		}

		dir.Dirs = append(dir.Dirs[:posn], dir.Dirs[posn+1:]...)
		dir.MarkDirty()
		dir.WriteBlock(dir, *dir.SafeRoot().Store)
		if err != nil {
			return fuse.EIO
		}

		return nil
	}

	return fuse.ENOSYS
}

func (dir *Dir) Create(req *fuse.CreateRequest, resp *fuse.CreateResponse, intr fs.Intr) (fs.Node, fs.Handle, fuse.Error) {
	if glog.V(2) {
		glog.Infof("Creating file %s %d", req.Name, len(req.Name))
	}

	if len(req.Name) > 255 {
		return nil, nil, fuse.Errno(syscall.ENAMETOOLONG)
	}

	dir.Lock.Lock()
	defer dir.Lock.Unlock()

	_, _, _, err := dir.LookupUnlocked(req.Name, intr)
	if err != fuse.ENOENT {
		return nil, nil, fuse.Errno(syscall.EEXIST)
	}

	blk := Block{Name: req.Name, Id: rand.Int63()}

	newFile := &File{Block: blk, Blocks: []Block{}, Root: dir.SafeRoot()}
	newFile.MarkDirty()
	err = newFile.WriteBlock(newFile, *dir.SafeRoot().Store)
	if err != nil {
		return nil, nil, fuse.EIO
	}

	dir.Files = append(dir.Files, blk)
	dir.MarkDirty()
	dir.WriteBlock(dir, *dir.SafeRoot().Store)
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
