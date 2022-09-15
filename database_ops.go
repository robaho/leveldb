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
	value, err = db.getMulti().Get(key)
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

	_, err := db.memory.Put(key, value)
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

	db.memory.Remove(key)
	return value, nil
}

// Lookup finds matching record between lower and upper inclusive. lower or upper can be nil
// and then the range is unbounded on that side. Using the iterator after the transaction has
// been Commit/Rollback is not supported.
func (db *Database) Lookup(lower []byte, upper []byte) (LookupIterator, error) {
	if !db.open {
		return nil, DatabaseClosed
	}
	itr, err := db.getMulti().Lookup(lower, upper)
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

	return db.memory.Write(wb)
}

func (db *Database) maybeSwapMemory() {
	if db.memory.bytes > db.options.MaxMemoryBytes {
		db.segments = copyAndAppend(db.segments, db.memory)
		db.memory = newMemorySegment(db.path, db.nextSegmentID(), db.options)
		db.multi = newMultiSegment(copyAndAppend(db.segments, db.memory))
	}
}

func (db *Database) maybeMerge() {
	if len(db.getSegments()) > 2*db.options.MaxSegments {
		mergeSegments0(db, db.options.MaxSegments)
	}
}
