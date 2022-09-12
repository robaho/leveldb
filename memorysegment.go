package leveldb

import (
	"path/filepath"
	"runtime"
)

// memorySegment wraps an im-memory skip list, so the number of items that can be inserted or removed
// in a transaction is limited by available memory. The skip list uses a nil Value to designate a key that
// has been removed from the table
type memorySegment struct {
	list  SkipList[KeyValue]
	log   *logFile
	id    uint64
	bytes int
	path  string
}

func newMemorySegment(path string, id uint64) *memorySegment {
	ms := new(memorySegment)
	ms.list = NewSkipList(KeyValueCompare)
	ms.id = id
	ms.path = path

	return ms
}

func (ms *memorySegment) ID() uint64 {
	return ms.id
}

func (ms *memorySegment) maybeCreateLogFile() error {
	if ms.log != nil {
		return nil
	}
	if ms.path == "" {
		return nil
	}
	log, err := newLogFile(ms.path, ms.id)
	if err != nil {
		return err
	}
	ms.log = log
	return nil
}

func (ms *memorySegment) Put(key []byte, value []byte) ([]byte, error) {
	err := ms.maybeCreateLogFile()
	if err != nil {
		return nil, err
	}
	prev := ms.list.Put(KeyValue{key: key, value: value})
	ms.bytes += len(key) + len(value) - len(prev.key) - len(prev.value)
	if ms.log != nil {
		err = ms.log.Write(key, value)
		if err != nil {
			return prev.value, err
		}
	}
	return prev.value, nil
}
func (ms *memorySegment) Get(key []byte) ([]byte, error) {
	value, ok := ms.list.get(Key(key))
	if !ok {
		return nil, KeyNotFound
	}
	return value.value, nil
}

func (ms *memorySegment) Remove(key []byte) ([]byte, error) {
	return ms.Put(key, nil)
}

func (ms *memorySegment) Write(wb WriteBatch) error {

	err := ms.maybeCreateLogFile()
	if err != nil {
		return err
	}

	if ms.log != nil {
		err = ms.log.StartBatch(len(wb.entries))
		if err != nil {
			return err
		}
	}

	for _, kv := range wb.entries {
		prev := ms.list.Put(kv)
		ms.bytes += len(kv.key) + len(kv.value) - len(prev.key) - len(prev.value)
		if ms.log != nil {
			err := ms.log.Write(kv.key, kv.value)
			if err != nil {
				return err
			}
		}
	}
	if ms.log != nil {
		err := ms.log.EndBatch(len(wb.entries))
		if err != nil {
			return err
		}
	}
	return nil
}

func (ms *memorySegment) Lookup(lower []byte, upper []byte) (LookupIterator, error) {
	itr := ms.list.Iterator()
	if lower != nil {
		itr.seek(Key(lower))
	} else {
		itr.seekToFirst()
	}
	return &skiplistIterator{itr: itr, lower: Key(lower), upper: Key(upper)}, nil
}

func (ms *memorySegment) Close() error {
	if ms.log != nil {
		err := ms.log.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (ms *memorySegment) removeSegment() error {
	var err0, err1 error
	err0 = ms.Close()
	if ms.log != nil {
		err1 = ms.log.Remove()
	}
	return errn(err0, err1)
}

func (ms *memorySegment) removeOnFinalize() {
	runtime.SetFinalizer(ms, func(ms *memorySegment) { ms.removeSegment() })
}

func (ms *memorySegment) files() []string {
	if ms.log != nil {
		return []string{filepath.Base(ms.log.file.Name())}
	} else {
		return []string{}
	}
}

type skiplistIterator struct {
	itr   iterator[KeyValue]
	lower KeyValue
	upper KeyValue
}

func (es *skiplistIterator) Next() (key []byte, value []byte, err error) {
	if !es.itr.valid() {
		return nil, nil, EndOfIterator
	}
	k := es.itr.key()
	if es.upper.key != nil && KeyValueCompare(k, es.upper) > 0 {
		return nil, nil, EndOfIterator
	}
	defer es.itr.next()
	return k.key, k.value, nil
}

func (es *skiplistIterator) peekKey() ([]byte, error) {
	if !es.itr.valid() {
		return nil, EndOfIterator
	}
	k := es.itr.key()
	if es.upper.key != nil && KeyValueCompare(k, es.upper) > 0 {
		return nil, EndOfIterator
	}
	return k.key, nil
}
