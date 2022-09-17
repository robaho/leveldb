package leveldb

import (
	"os"
	"path/filepath"
	"runtime"
)

// logSegment is a read-only segment created from a previous run but not yet merged
type logSegment struct {
	list SkipList[KeyValue]
	id   uint64
	path string
}

func newLogSegment(path string, options Options) (segment, error) {
	ls := new(logSegment)

	list, err := readLogFile(path, options)
	if err != nil {
		return nil, err
	}
	ls.list = *list
	ls.id = getSegmentID(path)
	ls.path = path

	return ls, nil
}

func (ls *logSegment) LowerID() uint64 {
	return ls.id
}
func (ls *logSegment) UpperID() uint64 {
	return ls.id
}

func (ls *logSegment) Get(key []byte) ([]byte, error) {
	value, ok := ls.list.get(Key(key))
	if !ok {
		return nil, KeyNotFound
	}
	return value.value, nil
}

func (ls *logSegment) Put(key []byte, value []byte) ([]byte, error) {
	panic("Put called on immutable logSegment")
}
func (ls *logSegment) Write(wb WriteBatch) error {
	panic("Write called on immutable logSegment")
}
func (ls *logSegment) Remove(key []byte) ([]byte, error) {
	panic("Remove called on immutable logSegment")
}

func (ls *logSegment) Lookup(lower []byte, upper []byte) (LookupIterator, error) {
	itr := ls.list.Iterator()
	if lower != nil {
		itr.seek(Key(lower))
	} else {
		itr.seekToFirst()
	}
	return &skiplistIterator{itr: itr, lower: Key(lower), upper: Key(upper), cmp: ls.list.cmp_}, nil
}

func (ls *logSegment) Close() error {
	return nil
}

func (ls *logSegment) removeSegment() error {
	var err0, err1 error
	err0 = ls.Close()
	err1 = os.Remove(ls.path)
	return errn(err0, err1)
}

func (ls *logSegment) removeOnFinalize() {
	runtime.SetFinalizer(ls, func(ls *logSegment) { ls.removeSegment() })
}

func (ls *logSegment) files() []string {
	return []string{filepath.Base(ls.path)}
}
