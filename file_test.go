package gobuddyfs

import (
	"testing"

	"github.com/stretchr/testify/mock"

	"bazil.org/fuse"
)

type MockBlockGenerator struct {
	mock.Mock
}

func (m *MockBlockGenerator) NewBlock() Block {
	args := m.Mock.Called()
	return args.Get(0).(Block)
}

func (m *MockBlockGenerator) NewNamedBlock(name string) Block {
	args := m.Mock.Called(name)
	return args.Get(0).(Block)
}

func BenchmarkFileWriteToCache(b *testing.B) {
	var bSize int64 = 4096
	var i int64

	file := &File{Block: Block{}, Blocks: []Block{}, blkGen: new(RandomizedBlockGenerator)}

	var data []byte = make([]byte, bSize)
	for i = 0; i < bSize; i++ {
		data[i] = '0'
	}

	req := &fuse.WriteRequest{Data: data, Offset: 0}
	res := &fuse.WriteResponse{}

	b.ResetTimer()

	b.N = min(b.N, 500000)
	for i := 0; i < b.N; i++ {
		file.Write(req, res, nil)
		req.Offset += bSize
	}
}

func TestFileSetSize(t *testing.T) {
	var mBlkGen = new(MockBlockGenerator)
	var file = &File{Block: Block{}, Blocks: []Block{}, blkGen: mBlkGen}

	mBlkGen.On("NewBlock").Return(Block{}).Once()
	file.setSize(4095)
	mBlkGen.AssertExpectations(t)

	mBlkGen.On("NewBlock").Return(Block{}).Twice()
	file.setSize(12288)
	mBlkGen.AssertExpectations(t)

	file.setSize(10000)
	mBlkGen.AssertExpectations(t)

	// TODO: Mock Block to track when a Block gets deleted.
	file.setSize(4096)
	mBlkGen.AssertExpectations(t)
}
