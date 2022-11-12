package leveldb

import "errors"

var KeyNotFound = errors.New("key not found")
var KeyTooLong = errors.New("key too long, max 1024")
var EmptyKey = errors.New("key is empty")
var DatabaseClosed = errors.New("database closed")
var DatabaseInUse = errors.New("database in use")
var SnapshotClosed = errors.New("snapshot closed")
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

// MapError maps err to a leveldb error, or returns a new error for err
func MapError(err string) error {
	switch err {
	case KeyNotFound.Error():
		return KeyNotFound
	case KeyTooLong.Error():
		return KeyTooLong
	case EmptyKey.Error():
		return EmptyKey
	case DatabaseClosed.Error():
		return DatabaseClosed
	case DatabaseInUse.Error():
		return DatabaseInUse
	case SnapshotClosed.Error():
		return SnapshotClosed
	case NoDatabaseFound.Error():
		return NoDatabaseFound
	case DatabaseCorrupted.Error():
		return DatabaseCorrupted
	case NotADirectory.Error():
		return NotADirectory
	case EndOfIterator.Error():
		return EndOfIterator
	case ReadOnlySegment.Error():
		return ReadOnlySegment
	default:
		return errors.New(err)
	}
}
