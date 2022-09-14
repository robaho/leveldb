package leveldb

import (
	"bytes"
	"github.com/nightlyone/lockfile"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"sync/atomic"
)

const dbMemorySegment = 1024 * 1024
const dbMaxSegments = 8

// Database reference is obtained via Open()
type Database struct {
	sync.Mutex
	segments  []segment
	memory    *memorySegment
	multi     segment
	open      bool
	closing   bool
	merger    chan bool
	deleter   Deleter
	path      string
	wg        sync.WaitGroup
	nextSegID uint64
	lockfile  lockfile.Lockfile
	options   Options

	// if non-nil an asynchronous error has occurred, and the database cannot be used
	err error
}

type Options struct {
	CreateIfNeeded bool
	DisableBgMerge bool
	// Maximum number of segments per database. Controls the number of open files.
	MaxSegments int
	// Maximum size of memory segment in bytes.
	MaxMemoryBytes int
	// Disable flush to disk when writing to increase performance.
	DisableWriteFlush bool
	// Force sync to disk when writing. If true, then DisableWriteFlush is ignored.
	EnableSyncWrite bool
}

// LookupIterator iterator interface for table scanning. all iterators should be read until completion
type LookupIterator interface {
	// Next returns EndOfIterator when complete, if err is nil, then key and value are valid
	Next() (key []byte, value []byte, err error)
	// returns the next non-deleted key in the index
	peekKey() ([]byte, error)
}

var global_lock sync.RWMutex

// Open a database. The database can only be opened by a single process, but the *Database
// reference can be shared across Go routines. The path is a directory name.
// if createIfNeeded is true, them if the db doesn't exist it will be created.
func Open(path string, options Options) (*Database, error) {
	global_lock.Lock()
	defer global_lock.Unlock()

	db, err := open(path, options)
	if err == NoDatabaseFound && options.CreateIfNeeded == true {
		return create(path, options)
	}
	return db, err
}

func open(path string, options Options) (*Database, error) {

	path = filepath.Clean(path)

	err := IsValidDatabase(path)
	if err != nil {
		return nil, err
	}

	abs, err := filepath.Abs(path + "/lockfile")
	if err != nil {
		return nil, err
	}
	lf, err := lockfile.New(abs)
	if err != nil {
		return nil, err
	}
	err = lf.TryLock()
	if err != nil {
		return nil, DatabaseInUse
	}

	db := &Database{path: path, open: true}
	db.lockfile = lf

	db.deleter = newDeleter(path)

	err = db.deleter.deleteScheduled()
	if err != nil {
		return nil, err
	}

	db.segments, err = loadDiskSegments(path)
	if err != nil {
		return nil, err
	}

	maxSegID := uint64(0)
	for _, seg := range db.segments {
		if seg.UpperID() > maxSegID {
			maxSegID = seg.UpperID()
		}
	}
	atomic.StoreUint64(&db.nextSegID, uint64(maxSegID))
	db.memory = newMemorySegment(db.path, db.nextSegmentID(), db.options)
	db.multi = newMultiSegment(append(db.segments, db.memory))
	db.merger = make(chan bool)
	db.options = options

	if db.options.MaxMemoryBytes < dbMemorySegment {
		db.options.MaxMemoryBytes = dbMemorySegment
	}
	if db.options.MaxSegments < dbMaxSegments {
		db.options.MaxSegments = dbMaxSegments
	}

	if !options.DisableBgMerge {
		db.wg.Add(1)
		go mergeSegments(db)
	}

	return db, nil
}

func create(path string, options Options) (*Database, error) {
	path = filepath.Clean(path)

	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		return nil, err
	}

	return open(path, options)
}

// Remove the database, deleting all files. the caller must be able to
// gain exclusive multi to the database
func Remove(path string) error {
	global_lock.Lock()
	defer global_lock.Unlock()

	path = filepath.Clean(path)

	err := IsValidDatabase(path)
	if err != nil {
		return err
	}

	abs, err := filepath.Abs(path + "/lockfile")
	if err != nil {
		return err
	}
	lf, err := lockfile.New(abs)
	if err != nil {
		return err
	}
	err = lf.TryLock()
	if err != nil {
		return DatabaseInUse
	}

	return os.RemoveAll(path)
}

// IsValidDatabase checks if the path points to a valid database or empty directory (which is also valid)
func IsValidDatabase(path string) error {
	fi, err := os.Stat(path)
	if err != nil {
		return NoDatabaseFound
	}

	if !fi.IsDir() {
		return NotADirectory
	}

	infos, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}

	for _, f := range infos {
		if "lockfile" == f.Name() {
			continue
		}
		if "deleted" == f.Name() {
			continue
		}
		if f.Name() == filepath.Base(path) {
			continue
		}
		if matched, _ := regexp.Match("(log|keys|data)\\..*", []byte(f.Name())); !matched {
			return NotValidDatabase
		}
	}
	return nil
}

// Close the database. any memory segments are persisted to disk.
// The resulting segments are merged until the default maxSegments is reached
func (db *Database) Close() error {
	return db.CloseWithMerge(db.options.MaxSegments)
}

// CloseWithMerge closes the database with control of the segment count. if segmentCount is 0, then
// the merge process is skipped
func (db *Database) CloseWithMerge(segmentCount int) error {
	global_lock.Lock()
	defer global_lock.Unlock()
	if !db.open {
		return DatabaseClosed
	}
	err := db.err

	if err != nil {
		goto finish
	}

	db.closing = true
	close(db.merger)

	db.wg.Wait() // wait for background merger to exit

	db.Lock()
	db.segments = append(db.segments, db.memory)
	db.memory = nil
	db.Unlock()

	if segmentCount > 0 {
		db.err = mergeSegments0(db, segmentCount)
	}

	if db.err != nil {
		goto finish
	}

	// write any remaining memory segments to disk
	db.Lock()
	for _, s := range db.segments {
		ms, ok := s.(*memorySegment)
		if ok {
			db.wg.Add(1)
			go func(s *memorySegment) {
				err0 := writeSegmentToDisk(db, s)
				if err0 != nil && err != errEmptySegment {
					db.err = err0
				}
				db.wg.Done()
			}(ms)
		}
	}

	db.wg.Wait()

	for _, s := range db.segments {
		s.Close()
	}

	err = db.deleter.deleteScheduled()

finish:
	db.segments = []segment{}
	db.lockfile.Unlock()
	db.open = false

	return err
}

func (db *Database) nextSegmentID() uint64 {
	return atomic.AddUint64(&db.nextSegID, 1)
}

func less(a []byte, b []byte) bool {
	return bytes.Compare(a, b) < 0
}
func equal(a []byte, b []byte) bool {
	return bytes.Equal(a, b)
}
