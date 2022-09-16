package leveldb

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
	if value == nil {
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

// Lookup finds matching record between lower and upper inclusive. lower or upper can be nil
// and then the range is unbounded on that side. Using the iterator after the transaction has
// been Commit/Rollback is not supported.
func (db *Database) Lookup(lower []byte, upper []byte) (LookupIterator, error) {
	if !db.open {
		return nil, DatabaseClosed
	}
	itr, err := db.getState().multi.Lookup(lower, upper)
	if err != nil {
		return nil, err
	}
	return &dbLookup{LookupIterator: itr, db: db}, nil
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
	if state.memory.bytes > db.options.MaxMemoryBytes {
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
	if len(state.segments) > 2*db.options.MaxSegments {
		mergeSegments0(db, db.options.MaxSegments)
	}
}
