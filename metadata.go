package gobuddyfs

import (
	"math/rand"
	"strconv"
)

type Marshalable interface {
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
}

type Addressable interface {
	GetId() int64
	SetId(int64)
}

type Cacheable interface {
	MarkDirty()
	IsDirty() bool
}

type Storable interface {
	WriteBlock(m Marshalable, store KVStore) error
	ReadBlock(m Marshalable, store KVStore) error
	Delete(store KVStore)
}

type StorageUnit interface {
	Addressable
	Cacheable
	Storable
}

type Block struct {
	Name  string
	Id    int64
	dirty bool `json:"-"`
}

var _ StorageUnit = new(Block)

func (b *Block) Delete(store KVStore) {
	store.Set(strconv.FormatInt(b.Id, 10), nil)
}

func (b *Block) SetId(id int64) {
	b.Id = id
}

func (b *Block) MarkDirty() {
	b.dirty = true
}

func (b Block) GetId() int64 {
	return b.Id
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
	StorageUnit
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
	NewBlock() StorageUnit
	NewNamedBlock(name string) Block
}

type RandomizedBlockGenerator struct {
	// Implements: BlockGenerator
}

var _ BlockGenerator = new(RandomizedBlockGenerator)

func (r RandomizedBlockGenerator) NewBlock() StorageUnit {
	return &Block{Id: rand.Int63()}
}

func (r RandomizedBlockGenerator) NewNamedBlock(name string) Block {
	return Block{Id: rand.Int63(), Name: name}
}
