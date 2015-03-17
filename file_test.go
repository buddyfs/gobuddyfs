package gobuddyfs

import (
	"testing"

	"github.com/stretchr/testify/assert"
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

type MockStore struct {
	mock.Mock
}

var _ KVStore = new(MockStore)

func (m *MockStore) Get(key string, retry bool) ([]byte, error) {
	args := m.Mock.Called(key, retry)
	return args.Get(0).([]byte), args.Error(1)
}

func (m *MockStore) Set(key string, value []byte) error {
	args := m.Mock.Called(key, value)
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
	mBlocks[1].AssertExpectations(t)
	mBlocks[2].AssertExpectations(t)
}

func TestFileWrite(t *testing.T) {
	var mBlkGen = new(MockBlockGenerator)
	var mStore = new(MockStore)
	var file = &File{Block: Block{}, Blocks: []StorageUnit{}, blkGen: mBlkGen,
		KVS: mStore}
	var mBlocks []*MockBlock = make([]*MockBlock, 3)

	mBlocks[0] = &MockBlock{}
	mBlocks[0].On("GetId").Return(int64(1))

	var bSize int64 = 4096
	var i int64

	var data []byte = make([]byte, bSize)
	for i = 0; i < bSize; i++ {
		data[i] = byte(i)
	}

	// Write first 1000 bytes
	req := &fuse.WriteRequest{Data: data[:1000], Offset: 0}
	res := &fuse.WriteResponse{}

	mBlkGen.On("NewBlock").Return(mBlocks[0]).Once()
	// Once for NewBlock and once more after writing data.
	mBlocks[0].On("MarkDirty").Return().Twice()
	file.Write(req, res, nil)
	mBlkGen.AssertExpectations(t)
	mBlocks[0].AssertExpectations(t)
	assert.EqualValues(t, 1000, file.Size)

	mBlocks[0].On("IsDirty").Return(true).Once()
	// TODO: Block output
	mBlocks[0].On("WriteBlock", mock.AnythingOfType("*gobuddyfs.DataBlock"),
		mStore).Return(nil).Once()
	// TODO: File layout output
	mStore.On("Set", "0", mock.Anything).Return(nil).Once()
	file.Flush(nil, nil)
	mBlkGen.AssertExpectations(t)
	mBlocks[0].AssertExpectations(t)

	// Write first 4096 bytes, overwriting previous content
	req = &fuse.WriteRequest{Data: data, Offset: 0}
	res = &fuse.WriteResponse{}

	mBlocks[0].On("MarkDirty").Return().Once()
	mStore.On("Get", "1", mock.Anything).Return(data[:1000], nil).Once()
	file.Write(req, res, nil)
	mBlkGen.AssertExpectations(t)
	mBlocks[0].AssertExpectations(t)
	assert.EqualValues(t, 4096, file.Size)

	// Overwrite bytes 200-299.
	req = &fuse.WriteRequest{Data: data[:100], Offset: 200}
	res = &fuse.WriteResponse{}

	mBlocks[0].On("MarkDirty").Return().Once()
	file.Write(req, res, nil)
	mBlkGen.AssertExpectations(t)
	mBlocks[0].AssertExpectations(t)
	assert.EqualValues(t, 4096, file.Size)
}
