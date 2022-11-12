package leveldb

// segment represents a portion(s) of the database, which is a "database" in and unto itself
// some operations are not supported on some segment types, as some are read-only
type segment interface {
	Put(key []byte, value []byte) ([]byte, error)
	Get(key []byte) ([]byte, error)
	Remove(key []byte) ([]byte, error)
	Lookup(lower []byte, upper []byte) (LookupIterator, error)
	Close() error
	LowerID() uint64
	UpperID() uint64
	removeSegment() error
	removeOnFinalize()
	files() []string
	size() uint64
}
