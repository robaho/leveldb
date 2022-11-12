package leveldb

import (
	"runtime"
)

// Special iterator to skip removed records.
type dbLookup struct {
	LookupIterator
	db *Database
}

func (dl *dbLookup) Next() (key, value []byte, err error) {
	for {
		if !dl.db.open {
			return nil, nil, DatabaseClosed
		}
		key, value, err = dl.LookupIterator.Next()
		if value == nil && err == nil {
			continue
		}
		return
	}
}

// Get a value for a key, error is non-nil if the key was not found or an error occurred
func (db *Database) Get(key []byte) (value []byte, err error) {
	if !db.open {
		return nil, DatabaseClosed
	}
	if len(key) > 1024 {
		return nil, KeyTooLong
	}
	value, err = db.getState().multi.Get(key)
	if err != nil {
		return nil, err
	}
	if value != nil && len(value) == 0 {
		return nil, KeyNotFound
	}
	return
}

// Put a key/value pair into the table, overwriting any existing entry. empty keys are not supported.
func (db *Database) Put(key []byte, value []byte) error {
	db.Lock()
	defer db.maybeMerge()
	defer db.Unlock()

	if !db.open {
		return DatabaseClosed
	}
	if len(key) > 1024 {
		return KeyTooLong
	}
	if len(key) == 0 {
		return EmptyKey
	}

	db.maybeSwapMemory()

	_, err := db.state.memory.Put(key, value)
	return err
}

// Remove a key and its value from the table. empty keys are not supported.
func (db *Database) Remove(key []byte) ([]byte, error) {
	db.Lock()
	defer db.maybeMerge()
	defer db.Unlock()

	if !db.open {
		return nil, DatabaseClosed
	}
	if len(key) > 1024 {
		return nil, KeyTooLong
	}
	value, err := db.Get(key)
	if err != nil {
		return nil, err
	}

	db.maybeSwapMemory()
	db.state.memory.Remove(key)
	return value, nil
}

// Lookup finds matching records between lower and upper inclusive. lower or upper can be nil
// and then the range is unbounded on that side. If the database is mutated during iteration, the returned results
// are undefined and are likely to be invalid in conjunction with a large number of mutations. A Snapshot should be
// used in this case.
func (db *Database) Lookup(lower []byte, upper []byte) (LookupIterator, error) {
	s, err := db.Snapshot()
	if err != nil {
		return nil, err
	}
	return s.Lookup(lower, upper)
}

// Snapshot creates a read-only view of the database at a moment in time.
func (db *Database) Snapshot() (*Snapshot, error) {
	db.Lock()
	defer db.Unlock()

	if !db.open {
		return nil, DatabaseClosed
	}

	state := db.getState()
	segments := copyAndAppend(state.segments, state.memory)
	memory := newMemorySegment(db.path, db.nextSegmentID(), db.options)
	multi := newMultiSegment(copyAndAppend(segments, memory))
	db.setState(&dbState{segments: segments, memory: memory, multi: multi})

	s := &Snapshot{
		db:    db,
		multi: newMultiSegment(segments),
	}
	db.snapshots = append(db.snapshots, s)
	runtime.SetFinalizer(s, func(s *Snapshot) { s.Close() })
	return s, nil
}

func (db *Database) Write(wb WriteBatch) error {
	db.Lock()
	defer db.maybeMerge()
	defer db.Unlock()

	if !db.open {
		return DatabaseClosed
	}

	db.maybeSwapMemory()

	return db.state.memory.Write(wb)
}

func (db *Database) maybeSwapMemory() {
	state := db.getState()
	if state.memory.size() > db.options.MaxMemoryBytes {
		segments := copyAndAppend(state.segments, state.memory)
		memory := newMemorySegment(db.path, db.nextSegmentID(), db.options)
		multi := newMultiSegment(copyAndAppend(segments, memory))
		db.setState(&dbState{segments: segments, memory: memory, multi: multi})
	}
}

func (db *Database) maybeMerge() {
	if db.options.DisableAutoMerge {
		return
	}
	state := db.getState()
	if len(state.segments) > int(2*db.options.MaxSegments) {
		mergeSegments0(db, db.options.MaxSegments)
	}
}
