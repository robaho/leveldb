package leveldb

// Snapshot is a read-only view of the database at a moment in time. A Snapshot can be used by multiple go routines,
// but access across Close() and other operations must be externally synchronized.
type Snapshot struct {
	db    *Database
	multi *multiSegment
}

func (s *Snapshot) Get(key []byte) ([]byte, error) {
	if !s.db.open {
		return nil, DatabaseClosed
	}
	value, err := s.multi.Get(key)
	if err != nil {
		return nil, err
	}
	if value != nil && len(value) == 0 {
		return nil, KeyNotFound
	}
	return value, nil
}

func (s *Snapshot) Lookup(lower []byte, upper []byte) (LookupIterator, error) {
	if s.multi == nil {
		return nil, SnapshotClosed
	}
	itr, err := s.multi.Lookup(lower, upper)
	if err != nil {
		return nil, err
	}
	return &dbLookup{LookupIterator: itr, db: s.db}, nil
}

// Close frees any resources used by the Snapshot. This is optional and instead simply setting the Snapshot reference
// to nil will eventually free the resources.
func (s *Snapshot) Close() {
	s.multi = nil
}
