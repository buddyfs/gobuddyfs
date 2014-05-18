package gobuddyfs_test

import (
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
	bfs := gobuddyfs.BuddyFS{Store: mkv}
	mkv.On("Get", "ROOT").Return(nil, fmt.Errorf("Something bad")).Once()
	node, err := bfs.Root()

	assert.Error(t, err)
	assert.Nil(t, node, "Error should return nil Root")

	mkv.AssertExpectations(t)
}

func TestRootCreateSuccess(t *testing.T) {
	mkv := new(MockKVStore)
	bfs := gobuddyfs.BuddyFS{Store: mkv}
	mkv.On("Get", "ROOT").Return(nil, nil).Once()
	mkv.On("Set", mock.Anything, mock.Anything).Return(nil).Twice()
	node, err := bfs.Root()

	assert.NoError(t, err)
	assert.NotNil(t, node, "Successfully created root should be non-nil")
	assert.NotNil(t, node.Attr())
	assert.Equal(t, node.Attr().Inode, uint64(1), "Root inode id")

	mkv.AssertExpectations(t)
}

func TestRootCreateWriteNodeFail(t *testing.T) {
	mkv := new(MockKVStore)
	bfs := gobuddyfs.BuddyFS{Store: mkv}
	mkv.On("Get", "ROOT").Return(nil, nil).Once()
	mkv.On("Set", mock.Anything, mock.Anything).Return(fmt.Errorf("Writing root node failed")).Once()
	node, err := bfs.Root()

	assert.Error(t, err)
	assert.Nil(t, node, "Error should return nil Root")

	mkv.AssertExpectations(t)
}

func TestRootCreateWriteROOTKeyFail(t *testing.T) {
	mkv := new(MockKVStore)
	bfs := gobuddyfs.BuddyFS{Store: mkv}
	mkv.On("Get", "ROOT").Return(nil, nil).Once()
	mkv.On("Set", mock.Anything, mock.Anything).Return(nil).Once()
	mkv.On("Set", mock.Anything, mock.Anything).Return(fmt.Errorf("Writing ROOT key failed")).Once()
	node, err := bfs.Root()

	assert.Error(t, err)
	assert.Nil(t, node, "Error should return nil Root")

	mkv.AssertExpectations(t)
}
