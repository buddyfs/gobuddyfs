package gobuddyfs_test

import (
	"encoding/binary"
	"fmt"
	"testing"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"

	"github.com/anupcshan/gobuddyfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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

func TestMkdirWithDuplicate(t *testing.T) {
	memkv := gobuddyfs.NewMemStore()
	bfs := gobuddyfs.NewBuddyFS(memkv)

	root, _ := bfs.Root()

	node, err := root.(*gobuddyfs.FSMeta).Mkdir(&fuse.MkdirRequest{Name: "foo"}, make(fs.Intr))
	assert.NoError(t, err)
	assert.NotNil(t, node, "Newly created directory node should not be nil")

	// Create duplicate directory
	node, err = root.(*gobuddyfs.FSMeta).Mkdir(&fuse.MkdirRequest{Name: "foo"}, make(fs.Intr))
	assert.Error(t, err, "Duplicate directory name")
	assert.Nil(t, node)
}

func TestParallelMkdirWithDuplicate(t *testing.T) {
	memkv := gobuddyfs.NewMemStore()
	bfs := gobuddyfs.NewBuddyFS(memkv)

	root, _ := bfs.Root()

	var node1, node2 fs.Node
	var err1, err2 fuse.Error
	done1 := make(chan bool)
	done2 := make(chan bool)

	mkdir := func(node *fs.Node, err *fuse.Error, done chan bool) {
		*node, *err = root.(*gobuddyfs.FSMeta).Mkdir(&fuse.MkdirRequest{Name: "foo"}, make(fs.Intr))
		done <- true
	}

	go mkdir(&node1, &err1, done1)
	go mkdir(&node2, &err2, done2)

	<-done1
	<-done2

	/*
	 * Constraints:
	 * - Exactly one of Node1 and Node2 will be non-nil and the other should be nil
	 * - For each pair of Node_i, Err_i, exactly one of them should be non-nil and the other should be nil
	 */

	if node1 != nil {
		t.Logf("Node1 succeeded")
	} else {
		t.Logf("Node2 succeeded")
	}

	if node1 == nil && node2 == nil {
		t.Errorf("Neither mkdir invocation succeeded while exactly should have.")
	} else if node1 != nil && node2 != nil {
		t.Errorf("Both mkdir invocations succeeded while exactly should have.")
	}

	if node1 == nil && err1 == nil {
		t.Errorf("Both node and err are nil while exactly one should have been non-nil")
	} else if node1 != nil && err1 != nil {
		t.Errorf("Both node and err are non-nil while exactly one should have been non-nil")
	}

	if node2 == nil && err2 == nil {
		t.Errorf("Both node and err are nil while exactly one should have been non-nil")
	} else if node2 != nil && err2 != nil {
		t.Errorf("Both node and err are non-nil while exactly one should have been non-nil")
	}
}
