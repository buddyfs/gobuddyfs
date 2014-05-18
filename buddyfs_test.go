package gobuddyfs_test

import (
	"encoding/binary"
	"fmt"
	"github.com/anupcshan/gobuddyfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
)

type MockKVStore struct {
	mock.Mock
	gobuddyfs.KVStore
}

func (m *MockKVStore) Get(key string) ([]byte, error) {
	args := m.Mock.Called(key)
	bytes, ok := args.Get(0).([]byte)
	if !ok {
		return nil, args.Error(1)
	}

	return bytes, args.Error(1)
}

func (m *MockKVStore) Set(key string, value []byte) error {
	args := m.Mock.Called(key, value)
	return args.Error(0)
}

func TestRootGetNodeError(t *testing.T) {
	mkv := new(MockKVStore)
	bfs := gobuddyfs.NewBuddyFS(mkv)
	mkv.On("Get", "ROOT").Return(nil, fmt.Errorf("Something bad")).Once()
	node, err := bfs.Root()

	assert.Error(t, err)
	assert.Nil(t, node, "Error should return nil Root")

	mkv.AssertExpectations(t)
}

func TestRootCreateSuccess(t *testing.T) {
	mkv := new(MockKVStore)
	bfs := gobuddyfs.NewBuddyFS(mkv)
	mkv.On("Get", "ROOT").Return(nil, nil).Once()
	mkv.On("Set", mock.Anything, mock.Anything).Return(nil).Twice()
	node, err := bfs.Root()

	assert.NoError(t, err)
	assert.NotNil(t, node, "Successfully created root should be non-nil")
	assert.NotNil(t, node.Attr())
	assert.Equal(t, node.Attr().Inode, uint64(1), "Root inode id")

	mkv.AssertExpectations(t)
}

func TestRootCreateAndReadRoot(t *testing.T) {
	mkv := new(MockKVStore)
	bfs := gobuddyfs.NewBuddyFS(mkv)
	mkv.On("Get", "ROOT").Return(nil, nil).Once()
	mkv.On("Set", mock.Anything, mock.Anything).Return(nil).Twice()
	node, err := bfs.Root()

	assert.NoError(t, err)
	assert.NotNil(t, node, "Successfully created root should be non-nil")
	assert.NotNil(t, node.Attr())
	assert.Equal(t, node.Attr().Inode, uint64(1), "Root inode id")

	mkv.AssertExpectations(t)

	node2, err2 := bfs.Root()
	assert.NoError(t, err2)
	assert.NotNil(t, node2)
	assert.Equal(t, node2, node, "Cached node")

	mkv.AssertExpectations(t)
}

func TestRootCreateWriteNodeFail(t *testing.T) {
	mkv := new(MockKVStore)
	bfs := gobuddyfs.NewBuddyFS(mkv)
	mkv.On("Get", "ROOT").Return(nil, nil).Once()
	mkv.On("Set", mock.Anything, mock.Anything).Return(fmt.Errorf("Writing root node failed")).Once()
	node, err := bfs.Root()

	assert.Error(t, err)
	assert.Nil(t, node, "Error should return nil Root")

	mkv.AssertExpectations(t)
}

func TestRootCreateWriteROOTKeyFail(t *testing.T) {
	mkv := new(MockKVStore)
	bfs := gobuddyfs.NewBuddyFS(mkv)
	mkv.On("Get", "ROOT").Return(nil, nil).Once()
	mkv.On("Set", mock.Anything, mock.Anything).Return(nil).Once()
	mkv.On("Set", mock.Anything, mock.Anything).Return(fmt.Errorf("Writing ROOT key failed")).Once()
	node, err := bfs.Root()

	assert.Error(t, err)
	assert.Nil(t, node, "Error should return nil Root")

	mkv.AssertExpectations(t)
}

func TestRootReadCorruptedExistingRootKey(t *testing.T) {
	mkv := new(MockKVStore)
	bfs := gobuddyfs.NewBuddyFS(mkv)

	buffer := []byte{255, 255, 255}

	mkv.On("Get", "ROOT").Return(buffer, nil).Once()
	node, err := bfs.Root()

	assert.Error(t, err)
	assert.Nil(t, node, "Error should return nil Root")

	mkv.AssertExpectations(t)
}

func TestRootReadCorruptedExistingRootNode(t *testing.T) {
	mkv := new(MockKVStore)
	bfs := gobuddyfs.NewBuddyFS(mkv)

	buffer := make([]byte, 80)
	id := int64(1000)
	binary.PutVarint(buffer, id)

	mkv.On("Get", "ROOT").Return(buffer, nil).Once()
	mkv.On("Get", "1000").Return(buffer, nil).Once()
	node, err := bfs.Root()

	assert.Error(t, err)
	assert.Nil(t, node, "Error should return nil Root")

	mkv.AssertExpectations(t)
}

func TestRootReadExistingRoot(t *testing.T) {
	mkv := new(MockKVStore)
	bfs := gobuddyfs.NewBuddyFS(mkv)

	buffer := make([]byte, 80)
	id := int64(2000)
	binary.PutVarint(buffer, id)

	jsonDir := "{\"name\": \"x\", \"Inode\": 1, \"Id\": 2000}"

	mkv.On("Get", "ROOT").Return(buffer, nil).Once()
	mkv.On("Get", "2000").Return([]byte(jsonDir), nil).Once()
	node, err := bfs.Root()

	assert.NoError(t, err)
	assert.NotNil(t, node, "Successfully created root should be non-nil")
	assert.NotNil(t, node.Attr())
	assert.Equal(t, node.Attr().Inode, uint64(1), "Root inode id")

	mkv.AssertExpectations(t)
}

func TestRootReReadExistingRoot(t *testing.T) {
	mkv := new(MockKVStore)
	bfs := gobuddyfs.NewBuddyFS(mkv)

	buffer := make([]byte, 80)
	id := int64(2000)
	binary.PutVarint(buffer, id)

	jsonDir := "{\"NextInode\": 2, \"name\": \"x\", \"Inode\": 1, \"Id\": 3000}"

	mkv.On("Get", "ROOT").Return(buffer, nil).Once()
	mkv.On("Get", "2000").Return([]byte(jsonDir), nil).Once()
	node, err := bfs.Root()

	assert.NoError(t, err)
	assert.NotNil(t, node, "Successfully created root should be non-nil")
	assert.NotNil(t, node.Attr())
	assert.Equal(t, node.Attr().Inode, uint64(1), "Root inode id")

	node2, err2 := bfs.Root()
	assert.NoError(t, err2)
	assert.NotNil(t, node2)
	assert.Equal(t, node2, node, "Cached node")

	mkv.AssertExpectations(t)
}
