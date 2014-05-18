package gobuddyfs

import (
	"encoding/json"
	"os"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/golang/glog"
)

// BuddyFS implements the Buddy file system.
type BuddyFS struct {
	Store KVStore

	fs.FS
}

func (self BuddyFS) Root() (fs.Node, fuse.Error) {
	// Switch to mapping ROOT -> rootNodeKey
	// Read encoded root node with key rootNodeKey
	rootEncoded, err := self.Store.Get("ROOT")
	if err != nil {
		root := Dir{}
		rootEncoded, err = json.Marshal(root)
		if err == nil {
			err = self.Store.Set("ROOT", rootEncoded)
			if err == nil {
				return root, nil
			} else {
				glog.Error(err)
				return nil, fuse.ENODATA
			}
		} else {
			glog.Error(err)
			return nil, fuse.ENODATA
		}
	}

	var root Dir
	err = json.Unmarshal(rootEncoded, root)

	if err != nil {
		return nil, fuse.ENODATA
	}

	return root, nil
}

// Dir implements both Node and Handle for the root directory.
type Dir struct {
}

func (Dir) Attr() fuse.Attr {
	return fuse.Attr{Inode: 1, Mode: os.ModeDir | 0555}
}

func (Dir) Lookup(name string, intr fs.Intr) (fs.Node, fuse.Error) {
	if name == "hello" {
		return File{}, nil
	}
	return nil, fuse.ENOENT
}

var dirDirs = []fuse.Dirent{
	{Inode: 2, Name: "hello", Type: fuse.DT_File},
}

func (Dir) ReadDir(intr fs.Intr) ([]fuse.Dirent, fuse.Error) {
	return dirDirs, nil
}

// File implements both Node and Handle for the hello file.
type File struct{}

func (File) Attr() fuse.Attr {
	return fuse.Attr{Inode: 2, Mode: 0444}
}

func (File) ReadAll(intr fs.Intr) ([]byte, fuse.Error) {
	return []byte("hello, world\n"), nil
}
