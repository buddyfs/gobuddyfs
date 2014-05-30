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
	Size   uint64
	Blocks []Block
	Root   *FSMeta `json:"-"`
}

func (file *File) Open(req *fuse.OpenRequest, res *fuse.OpenResponse, intr fs.Intr) (fs.Handle, fuse.Error) {
	glog.Infoln("Open called")
	return file, nil
}

func (file *File) Setattr(req *fuse.SetattrRequest, res *fuse.SetattrResponse, intr fs.Intr) fuse.Error {
	glog.Infoln("Setattr called")
	glog.Infoln("Req: ", req)

	valid := req.Valid
	if valid.Size() && req.Size != file.Size {
		// Resizing!
		if file.Size > req.Size {
			glog.Infoln("Shrinking")
			numBlocks := req.Size / BLOCK_SIZE

			if numBlocks < uint64(len(file.Blocks)) {
				blocksToDelete := file.Blocks[numBlocks:]
				file.Blocks = file.Blocks[:numBlocks]

				for blk := range blocksToDelete {
					glog.Warningln("Removing ", blocksToDelete[blk].Id)
				}
			}
		} else {
			glog.Warningln("TODO: Expanding")
		}
		file.Size = req.Size

		err := file.WriteBlock(file, *file.Root.Store)
		if err != nil {
			return fuse.EIO
		}

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

func (file *File) Marshal() ([]byte, error) {
	return json.Marshal(file)
}

func (file File) Attr() fuse.Attr {
	glog.Infoln("Attr called")
	return fuse.Attr{Mode: 0444, Blocks: uint64(len(file.Blocks)), Size: file.Size}
}

func (file *File) Release(req *fuse.ReleaseRequest, intr fs.Intr) fuse.Error {
	glog.Infoln("Release", file.Name)
	return nil
}

func (file *File) Forget() {
	glog.Infoln("FORGET", file.Name)
}

func (file *File) Flush(req *fuse.FlushRequest, intr fs.Intr) fuse.Error {
	glog.Infoln("FLUSH", file.Name)
	return nil
}

func (file *File) Read(req *fuse.ReadRequest, res *fuse.ReadResponse, intr fs.Intr) fuse.Error {
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
