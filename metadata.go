package gobuddyfs

import (
	"math/rand"
	"strconv"
)

type Marshalable interface {
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
}

type Block struct {
	Name  string
	Id    int64
	dirty bool `json:"-"`
}

func (b *Block) Delete(store KVStore) {
	store.Set(strconv.FormatInt(b.Id, 10), nil)
}

func (b *Block) MarkDirty() {
	b.dirty = true
}

func (b Block) IsDirty() bool {
	return b.dirty
}

func (b *Block) WriteBlock(m Marshalable, store KVStore) error {
	// Don't make a write if not dirty
	if b.dirty == false {
		return nil
	}

	bEncoded, err := m.Marshal()
	if err != nil {
		return err
	}

	err = store.Set(strconv.FormatInt(b.Id, 10), bEncoded)
	if err == nil {
		b.dirty = false
	}
	return err
}

func (b *Block) ReadBlock(m Marshalable, store KVStore) error {
	encoded, err := store.Get(strconv.FormatInt(b.Id, 10), true)
	if err != nil {
		return err
	}

	err = m.Unmarshal(encoded)

	if err != nil {
		return err
	}

	b.dirty = false

	return nil
}

type DataBlock struct {
	Block
	Data []byte
}

var _ Marshalable = new(DataBlock)

func (dBlock DataBlock) Marshal() ([]byte, error) {
	return dBlock.Data, nil
}

func (dBlock *DataBlock) Unmarshal(data []byte) error {
	dBlock.Data = data
	return nil
}

type BlockGenerator interface {
	NewBlock() Block
	NewNamedBlock(name string) Block
}

type RandomizedBlockGenerator struct {
	// Implements: BlockGenerator
}

var _ BlockGenerator = new(RandomizedBlockGenerator)

func (r RandomizedBlockGenerator) NewBlock() Block {
	return Block{Id: rand.Int63()}
}

func (r RandomizedBlockGenerator) NewNamedBlock(name string) Block {
	return Block{Id: rand.Int63(), Name: name}
}
