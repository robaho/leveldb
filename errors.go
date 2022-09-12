package leveldb

import "errors"

var KeyNotFound = errors.New("key not found")
var KeyTooLong = errors.New("key too long, max 1024")
var EmptyKey = errors.New("key is empty")
var DatabaseClosed = errors.New("database closed")
var DatabaseInUse = errors.New("database in use")
var NoDatabaseFound = errors.New("no database found")
var DatabaseCorrupted = errors.New("database corrupted, run repair")
var NotADirectory = errors.New("path is not a directory")
var NotValidDatabase = errors.New("path is not a valid database")
var EndOfIterator = errors.New("end of iterator")
var ReadOnlySegment = errors.New("read only segment")

// returns the first non-nil error
func errn(errs ...error) error {
	for _, v := range errs {
		if v != nil {
			return v
		}
	}
	return nil
}
