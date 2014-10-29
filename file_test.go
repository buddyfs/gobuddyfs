package gobuddyfs

import (
	"testing"

	"github.com/stretchr/testify/mock"

	"bazil.org/fuse"
)

type MockBlockGenerator struct {
	mock.Mock
}

var _ BlockGenerator = new(MockBlockGenerator)

func (m *MockBlockGenerator) NewBlock() StorageUnit {
	args := m.Mock.Called()
	return args.Get(0).(StorageUnit)
}

func (m *MockBlockGenerator) NewNamedBlock(name string) Block {
	args := m.Mock.Called(name)
	return args.Get(0).(Block)
}

type MockBlock struct {
	mock.Mock
}

var _ StorageUnit = new(MockBlock)

func (b *MockBlock) Delete(store KVStore) {
	b.Mock.Called(store)
}

func (b *MockBlock) GetId() int64 {
	args := b.Mock.Called()
	return args.Get(0).(int64)
}

func (b *MockBlock) IsDirty() bool {
	args := b.Mock.Called()
	return args.Bool(0)
}

func (b *MockBlock) MarkDirty() {
	b.Mock.Called()
}

func (b *MockBlock) ReadBlock(m Marshalable, store KVStore) error {
	args := b.Mock.Called(m, store)
	return args.Error(0)
}

func (b *MockBlock) SetId(id int64) {
	b.Mock.Called(id)
}

func (b *MockBlock) WriteBlock(m Marshalable, store KVStore) error {
	args := b.Mock.Called(m, store)
	return args.Error(0)
}

func BenchmarkFileWriteToCache(b *testing.B) {
	var bSize int64 = 4096
	var i int64

	file := &File{Block: Block{}, Blocks: []StorageUnit{}, blkGen: new(RandomizedBlockGenerator)}

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
	var file = &File{Block: Block{}, Blocks: []StorageUnit{}, blkGen: mBlkGen}
	var mBlocks []*MockBlock = make([]*MockBlock, 3)

	mBlocks[0] = &MockBlock{}
	mBlocks[0].On("MarkDirty").Return()
	mBlocks[0].On("GetId").Return(int64(1))

	mBlocks[1] = &MockBlock{}
	mBlocks[1].On("MarkDirty").Return()
	mBlocks[1].On("GetId").Return(int64(2))

	mBlocks[2] = &MockBlock{}
	mBlocks[2].On("MarkDirty").Return()
	mBlocks[2].On("GetId").Return(int64(3))

	mBlkGen.On("NewBlock").Return(mBlocks[0]).Once()
	file.setSize(4095)
	mBlkGen.AssertExpectations(t)

	mBlkGen.On("NewBlock").Return(mBlocks[1]).Once()
	mBlkGen.On("NewBlock").Return(mBlocks[2]).Once()
	file.setSize(12288)
	mBlkGen.AssertExpectations(t)

	file.setSize(10000)
	mBlkGen.AssertExpectations(t)

	mBlocks[1].On("Delete", nil).Return().Once()
	mBlocks[2].On("Delete", nil).Return().Once()
	file.setSize(4096)
	mBlkGen.AssertExpectations(t)
}
