package gobuddyfs

import (
	"encoding/binary"
	"encoding/json"
	"math/rand"
	"os"
	"strconv"

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
	rootKey, err := self.Store.Get("ROOT")

	if err != nil {
		// Error reading the key
		return nil, fuse.ENODATA
	} else if rootKey == nil {
		// Root key not found
		root := NewDir("/")
		err = root.Write(self.Store)
		if err == nil {
			buffer := make([]byte, 80)
			binary.PutVarint(buffer, root.Block.Id)
			err = self.Store.Set("ROOT", buffer)
			if err == nil {
				return root, nil
			} else {
				glog.Errorf("Error while creating root node: %q", err)
				return nil, fuse.ENODATA
			}
		} else {
			glog.Errorf("Error while creating root node: %q", err)
			return nil, fuse.ENODATA
		}
	}

	var root Dir
	var n int
	root.Block.Id, n = binary.Varint(rootKey)
	if n < 0 {
		glog.Errorf("Error while decoding root key")
		return nil, fuse.ENODATA
	}

	err = root.Read(self.Store)
	if err != nil {
		glog.Errorf("Error while read root block: %q", err)
		return nil, fuse.ENODATA
	}

	return root, nil
}

type Block struct {
	Id int64
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
	name string
	Block
}

func NewDir(name string) *Dir {
	return &Dir{name: name, Block: Block{Id: rand.Int63()}}
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
