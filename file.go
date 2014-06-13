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
	Size       uint64
	Blocks     []Block
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
	if int(index) > len(file.BlockCache) {
		return nil
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

func (file *File) Setattr(req *fuse.SetattrRequest, res *fuse.SetattrResponse, intr fs.Intr) fuse.Error {
	if glog.V(2) {
		glog.Infoln("Setattr called")
		glog.Infoln("Req: ", req)
	}

	valid := req.Valid
	if valid.Size() && req.Size != file.Size {
		// Resizing!
		if file.Size > req.Size {
			glog.Infoln("Shrinking")
			numBlocks := req.Size / BLOCK_SIZE

			if numBlocks < uint64(len(file.Blocks)) {
				blocksToDelete := file.Blocks[numBlocks:]
				file.Blocks = file.Blocks[:numBlocks]
				file.BlockCache = file.BlockCache[:numBlocks]

				for blk := range blocksToDelete {
					glog.Warningln("Removing ", blocksToDelete[blk].Id)
				}
			}
		} else {
			glog.Warningln("TODO: Expanding")
		}
		file.Size = req.Size

		err := file.WriteBlock(file, *file.SafeRoot().Store)
		if err != nil {
			return fuse.EIO
		}

		if glog.V(2) {
			glog.Infoln("Resizing file to size", file.Size)
		}
		res.Attr = file.Attr()
		return nil
	}

	res.Attr = file.Attr()
	if glog.V(2) {
		glog.Infoln("Finished Setattr")
	}
	// TODO: Not implemented.
	return nil
}

func (file *File) Write(req *fuse.WriteRequest, res *fuse.WriteResponse, intr fs.Intr) fuse.Error {
	dataBytes := len(req.Data)
	if glog.V(2) {
		glog.Infof("Writing %d byte(s) at offset %d", dataBytes, req.Offset)
	}
	for req.Offset+int64(dataBytes) >= int64(BLOCK_SIZE*len(file.Blocks)) {
		blk := Block{Id: rand.Int63()}
		dBlk := DataBlock{Block: blk, Data: []byte{}}
		dBlk.MarkDirty()

		file.Blocks = append(file.Blocks, blk)
		file.appendBlock(&dBlk)
		file.MarkDirty()
	}

	startBlockId := req.Offset / BLOCK_SIZE

	var startBlock *DataBlock = file.getBlock(startBlockId)

	if glog.V(2) {
		glog.Infof("Block content length: %d", len(startBlock.Data))
	}

	oldLen := len(startBlock.Data)
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
	file.Size = file.Size + uint64(len(startBlock.Data)) - uint64(oldLen)

	file.MarkDirty()
	return nil
}

func (file *File) Marshal() ([]byte, error) {
	return json.Marshal(file)
}

func (file File) Attr() fuse.Attr {
	if glog.V(2) {
		glog.Infoln("Attr called")
	}
	return fuse.Attr{Mode: 0444, Blocks: uint64(len(file.Blocks)), Size: file.Size}
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
