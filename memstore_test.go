package gobuddyfs_test

import (
	"bytes"
	"strconv"
	"sync"
	"testing"

	"github.com/buddyfs/gobuddyfs"
	"github.com/stretchr/testify/assert"
)

func TestGetSet(t *testing.T) {
	s := gobuddyfs.NewMemStore()
	bar := []byte("bar")
	s.Set("Foo", bar)

	r, err := s.Get("Foo", false)

	assert.NoError(t, err)
	assert.Equal(t, bar, r)
}

func TestParallelGetSet(t *testing.T) {
	s := gobuddyfs.NewMemStore()
	bar := []byte("bar")
	baz := []byte("baz")
	s.Set("Foo", bar)

	const parallelism = 1000
	results := make([][]byte, parallelism)
	errors := make([]error, parallelism)
	dones := make([]chan bool, parallelism)

	getter := func(res *[]byte, err *error, done chan bool) {
		*res, *err = s.Get("Foo", false)
		done <- true
	}

	for i := 0; i < parallelism; i++ {
		dones[i] = make(chan bool)
	}

	for i := 0; i < parallelism/2; i++ {
		go getter(&results[i], &errors[i], dones[i])
	}

	go func() {
		s.Set("Foo", baz)
	}()

	for i := parallelism / 2; i < parallelism; i++ {
		go getter(&results[i], &errors[i], dones[i])
	}

	for i := 0; i < parallelism; i++ {
		<-dones[i]
	}

	for i := 0; i < parallelism; i++ {
		assert.NoError(t, errors[i])
		if bytes.Equal(results[i], bar) || bytes.Equal(results[i], baz) {
			continue
		}

		t.Errorf("Result %d did not match bar or baz", i)
	}
}

func BenchmarkSets(b *testing.B) {
	s := gobuddyfs.NewMemStore()
	keyChan := make(chan string, b.N)

	bar := []byte("bar")
	const parallelism = 16
	keys := make([]string, parallelism)
	var wg sync.WaitGroup

	for i := 0; i < len(keys); i++ {
		keys[i] = strconv.Itoa(i % len(keys))
		wg.Add(1)

		go func() {
			for key := range keyChan {
				s.Set(key, bar)
			}
			wg.Done()
		}()
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		keyChan <- keys[i%parallelism]
	}

	close(keyChan)
	wg.Wait()
}
