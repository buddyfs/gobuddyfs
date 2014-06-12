package gobuddyfs

type KVStore interface {
	Get(string, bool) ([]byte, error)
	Set(string, []byte) error
}
