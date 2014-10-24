package gobuddyfs

import (
	"encoding/json"
	"math/rand"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/golang/glog"
)

// File implements both Node and Handle for the hello file.
type File struct {
	Block
	Blocks     []Block
	Size       uint64
	Root       *FSMeta      `json:"-"`
	BlockCache []*DataBlock `json:"-"`
	BFS        *BuddyFS     `json:"-"`
}

func (file *File) SafeRoot() *FSMeta {
	if file.Root != nil {
		return file.Root
	}
	rt, _ := file.BFS.Root()

	file.Root, _ = rt.(*FSMeta)
	return file.Root
}

func (file *File) Open(req *fuse.OpenRequest, res *fuse.OpenResponse, intr fs.Intr) (fs.Handle, fuse.Error) {
	if glog.V(2) {
		glog.Infoln("Open called")
	}
	return file, nil
}

func (file *File) getBlock(index int64) *DataBlock {
	glog.Infoln("GetBlock: ", index)

	if uint64(index) >= blkCount(file.Size, BLOCK_SIZE) {
		return nil
	}

	// TODO: This step is incredibly wasteful, especially for large files.
	// Switch to using a map with size bounds here. Alternately, use a global
	// cache with a simple list of dirty blockids stored here.
	for int64(len(file.BlockCache)) <= index {
		glog.Infoln("Adding empty entries in block cache")
		file.BlockCache = append(file.BlockCache, nil)
	}

	if file.BlockCache[index] == nil {
		var startBlock DataBlock
		startBlock.Block.Id = file.Blocks[index].Id
		err := startBlock.ReadBlock(&startBlock, *file.SafeRoot().Store)
		if err != nil {
			glog.Errorf("Error while reading data block: %q", err)
			return nil
		}

		file.BlockCache[index] = &startBlock
	}

	return file.BlockCache[index]
}

func (file *File) appendBlock(dblk *DataBlock) {
	if glog.V(2) {
		glog.Infoln("AppendBlock: ", len(file.BlockCache))
	}
	file.BlockCache = append(file.BlockCache, dblk)
	file.MarkDirty()
}

func blkCount(size uint64, BLK_SIZE uint64) uint64 {
	return (size + BLK_SIZE - 1) / BLK_SIZE
}

// TODO: Should the return type be a standard error instead?
// TODO: Unit tests!
func (file *File) setSize(size uint64) fuse.Error {
	newBlockCount := blkCount(size, BLOCK_SIZE)

	if newBlockCount < uint64(len(file.Blocks)) {
		glog.Infoln("Reducing number of blocks to", newBlockCount)
		blocksToDelete := file.Blocks[newBlockCount:]
		file.Blocks = file.Blocks[:newBlockCount]
		file.BlockCache = file.BlockCache[:newBlockCount]

		for blk := range blocksToDelete {
			// TODO: Actually call delete on the backing store
			glog.Warningln("Removing ", blocksToDelete[blk].Id)
		}
	} else if newBlockCount > uint64(len(file.Blocks)) {
		glog.Infoln("Increasing number of blocks to", newBlockCount)
		for uint64(len(file.Blocks)) < newBlockCount {
			blk := Block{Id: rand.Int63()}
			dBlk := DataBlock{Block: blk, Data: []byte{}}
			dBlk.MarkDirty()

			file.Blocks = append(file.Blocks, blk)
			file.appendBlock(&dBlk)
		}
	}

	// Else, the file size change did not change the number of blocks.
	// Essentially, the change was limited to the last block. In this case, we
	// simply change the size attribute and proceed.

	file.Size = size
	file.MarkDirty()

	return nil
}

func (file *File) Setattr(req *fuse.SetattrRequest, res *fuse.SetattrResponse, intr fs.Intr) fuse.Error {
	if glog.V(2) {
		glog.Infoln("Setattr called")
		glog.Infoln("Req: ", req)
	}

	metaChanges := false
	valid := req.Valid
	if valid.Size() && req.Size != file.Size {
		file.setSize(req.Size)
		metaChanges = true
	}

	if res.Attr != file.Attr() {
		res.Attr = file.Attr()
		metaChanges = true
	}

	// TODO: Handle or ignore metadata changes like uid/gid/timestamps.

	if metaChanges {
		// There are metadata changes to the file, write back before proceeding.
		return file.Flush(nil, intr)
	}

	return nil
}

func (file *File) Write(req *fuse.WriteRequest, res *fuse.WriteResponse, intr fs.Intr) fuse.Error {
	dataBytes := len(req.Data)
	if glog.V(2) {
		glog.Infof("Writing %d byte(s) at offset %d", dataBytes, req.Offset)
	}

	// In case we write past current EOF, expand the file.
	if uint64(req.Offset)+uint64(dataBytes) > file.Size {
		file.setSize(uint64(req.Offset) + uint64(dataBytes))
	}

	// TODO: Write currently only updates one block worth of data.
	startBlockId := req.Offset / BLOCK_SIZE

	var startBlock *DataBlock = file.getBlock(startBlockId)

	if glog.V(2) {
		glog.Infof("Block content length: %d", len(startBlock.Data))
	}

	bytesToAdd := min(BLOCK_SIZE-int(req.Offset%BLOCK_SIZE), dataBytes)
	data := append(startBlock.Data[:(req.Offset%BLOCK_SIZE)], req.Data[:bytesToAdd]...)
	if len(startBlock.Data) >= int(req.Offset%BLOCK_SIZE)+bytesToAdd {
		data = append(data, startBlock.Data[int(req.Offset%BLOCK_SIZE)+bytesToAdd:]...)
	}
	startBlock.Data = data

	if glog.V(2) {
		glog.Infof("Block content length after: %d", len(startBlock.Data))
	}

	startBlock.MarkDirty()

	if glog.V(2) {
		glog.Infoln("Successfully completed write operation")
	}

	res.Size = bytesToAdd
	file.MarkDirty()
	return nil
}

func (file *File) Marshal() ([]byte, error) {
	return json.Marshal(file)
}

func (file File) Attr() fuse.Attr {
	if glog.V(2) {
		glog.Infoln("Attr called", file.Name)
	}

	return fuse.Attr{Mode: 0444, Inode: uint64(file.Id),
		Blocks: uint64(len(file.Blocks)), Size: file.Size}
}

func (file *File) Release(req *fuse.ReleaseRequest, intr fs.Intr) fuse.Error {
	if glog.V(2) {
		glog.Infoln("Release", file.Name)
	}
	return nil
}

func (file *File) Forget() {
	if glog.V(2) {
		glog.Infoln("FORGET", file.Name)
	}
}

func (file *File) Flush(req *fuse.FlushRequest, intr fs.Intr) fuse.Error {
	if glog.V(2) {
		glog.Infoln("FLUSH", file.Name, file.IsDirty())
	}
	for i := range file.BlockCache {
		if file.BlockCache[i] != nil && file.BlockCache[i].IsDirty() {
			err := file.BlockCache[i].WriteBlock(file.BlockCache[i], *file.SafeRoot().Store)
			if err != nil {
				glog.Warning("Unable to write block %s due to error: %s", file.Blocks[i].Id, err)
			} else {
				file.BlockCache[i] = nil
			}
		}
	}

	if file.IsDirty() {
		file.WriteBlock(file, *file.SafeRoot().Store)
	}
	return nil
}

func (file *File) Fsync(req *fuse.FsyncRequest, intr fs.Intr) fuse.Error {
	if glog.V(2) {
		glog.Infoln("FSYNC", file.Name, file.IsDirty())
	}
	return file.Flush(nil, intr)
}

func (file *File) Read(req *fuse.ReadRequest, res *fuse.ReadResponse, intr fs.Intr) fuse.Error {
	if glog.V(2) {
		glog.Infof("Reading %d byte(s) at offset %d", req.Size, req.Offset)
	}

	if req.Offset >= int64(file.Size) {
		res.Data = []byte{}
		return nil
	}

	res.Data = []byte{}

	startBlockId := req.Offset / BLOCK_SIZE

	var startBlock *DataBlock = file.getBlock(startBlockId)
	if startBlock == nil {
		glog.Error("Error while reading block")
		return fuse.EIO
	}

	beginReadByte := int(req.Offset % BLOCK_SIZE)
	endReadByte := min(len(startBlock.Data)-beginReadByte, req.Size)
	if glog.V(2) {
		glog.Infof("Block content length: %d", len(startBlock.Data))
		glog.Infof("Reading from %d to %d in block %d", beginReadByte, endReadByte+beginReadByte, startBlockId)
	}
	res.Data = startBlock.Data[beginReadByte : endReadByte+beginReadByte]

	return nil
}
