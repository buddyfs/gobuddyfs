package gobuddyfs_test

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"testing"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"

	"github.com/buddyfs/gobuddyfs"
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

// FIXME: Re-enable this test after fixing error passing semantics in Get()
/*
func TestRootGetNodeError(t *testing.T) {
	mkv := new(MockKVStore)
	bfs := gobuddyfs.NewBuddyFS(mkv)
	mkv.On("Get", "ROOT").Return(nil, fmt.Errorf("Something bad")).Once()
	node, err := bfs.Root()

	assert.Error(t, err)
	assert.Nil(t, node, "Error should return nil Root")

	mkv.AssertExpectations(t)
}
*/

func TestRootCreateSuccess(t *testing.T) {
	mkv := new(MockKVStore)
	bfs := gobuddyfs.NewBuddyFS(mkv)
	mkv.On("Get", "ROOT").Return(nil, nil).Once()
	mkv.On("Set", mock.Anything, mock.Anything).Return(nil).Twice()
	node, err := bfs.Root()

	assert.NoError(t, err)
	assert.NotNil(t, node, "Successfully created root should be non-nil")
	assert.NotNil(t, node.Attr())

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

	const parallelism = 10
	nodes := make([]fs.Node, parallelism)
	errs := make([]fuse.Error, parallelism)
	dones := make([]chan bool, parallelism)

	for i := 0; i < parallelism; i++ {
		dones[i] = make(chan bool)
	}

	mkdir := func(node *fs.Node, err *fuse.Error, done chan bool) {
		*node, *err = root.(*gobuddyfs.FSMeta).Mkdir(&fuse.MkdirRequest{Name: "foo"}, make(fs.Intr))
		done <- true
	}

	for i := 0; i < parallelism; i++ {
		go mkdir(&nodes[i], &errs[i], dones[i])
	}

	for i := 0; i < parallelism; i++ {
		<-dones[i]
	}

	/*
	 * Constraints:
	 * - Exactly one of Node1 and Node2 will be non-nil and the other should be nil
	 * - For each pair of Node_i, Err_i, exactly one of them should be non-nil and the other should be nil
	 */

	nodesNonNil := 0
	for i := 0; i < parallelism; i++ {
		if nodes[i] != nil {
			nodesNonNil++
			t.Logf("Node[%d] succeeded", i)
		}
	}
	assert.Equal(t, 1, nodesNonNil, "Exactly one of the mkdirs should have succeeded")

	for i := 0; i < parallelism; i++ {
		if nodes[i] == nil && errs[i] == nil {
			t.Errorf("Both node and err are nil while exactly one should have been non-nil")
		} else if nodes[i] != nil && errs[i] != nil {
			t.Errorf("Both node and err are non-nil while exactly one should have been non-nil")
		}
	}
}

func TestCreateWithDuplicate(t *testing.T) {
	memkv := gobuddyfs.NewMemStore()
	bfs := gobuddyfs.NewBuddyFS(memkv)

	root, _ := bfs.Root()

	node, _, err := root.(*gobuddyfs.FSMeta).Create(&fuse.CreateRequest{Name: "foo"}, nil, make(fs.Intr))
	assert.NoError(t, err)
	assert.NotNil(t, node, "Newly created file node should not be nil")

	// Create duplicate file
	node, _, err = root.(*gobuddyfs.FSMeta).Create(&fuse.CreateRequest{Name: "foo"}, nil, make(fs.Intr))
	assert.Error(t, err, "Duplicate file name")
	assert.Nil(t, node)
}

func TestParallelCreateWithDuplicate(t *testing.T) {
	memkv := gobuddyfs.NewMemStore()
	bfs := gobuddyfs.NewBuddyFS(memkv)

	root, _ := bfs.Root()

	const parallelism = 10
	nodes := make([]fs.Node, parallelism)
	errs := make([]fuse.Error, parallelism)
	dones := make([]chan bool, parallelism)

	for i := 0; i < parallelism; i++ {
		dones[i] = make(chan bool)
	}

	mkdir := func(node *fs.Node, err *fuse.Error, done chan bool) {
		*node, _, *err = root.(*gobuddyfs.FSMeta).Create(&fuse.CreateRequest{Name: "foo"}, nil, make(fs.Intr))
		done <- true
	}

	for i := 0; i < parallelism; i++ {
		go mkdir(&nodes[i], &errs[i], dones[i])
	}

	for i := 0; i < parallelism; i++ {
		<-dones[i]
	}

	/*
	 * Constraints:
	 * - Exactly one of Node1 and Node2 will be non-nil and the other should be nil
	 * - For each pair of Node_i, Err_i, exactly one of them should be non-nil and the other should be nil
	 */

	nodesNonNil := 0
	for i := 0; i < parallelism; i++ {
		if nodes[i] != nil {
			nodesNonNil++
			t.Logf("Node[%d] succeeded", i)
		}
	}
	assert.Equal(t, 1, nodesNonNil, "Exactly one of the creates should have succeeded")

	for i := 0; i < parallelism; i++ {
		if nodes[i] == nil && errs[i] == nil {
			t.Errorf("Both node and err are nil while exactly one should have been non-nil")
		} else if nodes[i] != nil && errs[i] != nil {
			t.Errorf("Both node and err are non-nil while exactly one should have been non-nil")
		}
	}
}

func TestParallelCreate(t *testing.T) {
	memkv := gobuddyfs.NewMemStore()
	bfs := gobuddyfs.NewBuddyFS(memkv)

	root, _ := bfs.Root()

	const parallelism = 100
	nodes := make([]fs.Node, parallelism)
	errs := make([]fuse.Error, parallelism)
	dones := make([]chan bool, parallelism)

	for i := 0; i < parallelism; i++ {
		dones[i] = make(chan bool)
	}

	mkdir := func(node *fs.Node, err *fuse.Error, done chan bool, name string) {
		*node, _, *err = root.(*gobuddyfs.FSMeta).Create(&fuse.CreateRequest{Name: name}, nil, make(fs.Intr))
		done <- true
	}

	for i := 0; i < parallelism; i++ {
		go mkdir(&nodes[i], &errs[i], dones[i], "name"+strconv.Itoa(i))
	}

	for i := 0; i < parallelism; i++ {
		<-dones[i]
	}

	/*
	 * Constraints:
	 * - Exactly one of Node1 and Node2 will be non-nil and the other should be nil
	 * - For each pair of Node_i, Err_i, exactly one of them should be non-nil and the other should be nil
	 */

	for i := 0; i < parallelism; i++ {
		if nodes[i] != nil {
			assert.NoError(t, errs[i])
			assert.NotNil(t, nodes[i], "Newly created file node should not be nil")
		}
	}
}
