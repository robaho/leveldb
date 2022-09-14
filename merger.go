package leveldb

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"time"
)

// merge segments for the database
func mergeSegments(db *Database) {
	defer db.wg.Done()
	//defer fmt.Println("merger complete on "+db.path)

	for {
		select {
		case <-time.After(time.Second):
			break
		case <-db.merger:
			break
		}
		if db.closing || db.err != nil {
			return
		}

		// the following prevents a Close from occurring while this
		// routine is running

		db.wg.Add(1)

		err := mergeSegments0(db, db.options.MaxSegments)
		db.Lock()
		if err != nil {
			db.err = errors.New("unable to merge segments: " + err.Error())
		}
		db.Unlock()

		db.wg.Done()
	}
}

func mergeSegments0(db *Database, segmentCount int) error {
	// only a single routine can be in mergeSegments0 to avoid deadlock
	if !atomic.CompareAndSwapInt32(&db.inMerge, 0, 1) {
		return nil
	}
	defer atomic.StoreInt32(&db.inMerge, 0)

	//fmt.Println("merging segments", db.path)

	var index = 0

	for {

		db.Lock()
		segments := db.segments
		db.Unlock()

		if len(segments) <= segmentCount {
			return nil
		}

		maxMergeSize := len(segments) / 2
		if maxMergeSize < 4 {
			maxMergeSize = 4
		}

		// ensure that only valid disk segments are merged

		mergable := make([]segment, 0)

		for _, s := range segments[index:] {
			mergable = append(mergable, s)
			if len(mergable) == maxMergeSize {
				break
			}
		}

		if len(mergable) < 2 {
			if index == 0 {
				return nil
			}
			index = 0
			continue
		}

		segments = segments[index : index+len(mergable)]

		newseg, err := mergeSegments1(db.deleter, db.path, segments)
		if err != nil {
			return err
		}

		db.Lock() // need lock when updating db segments
		segments = db.segments

		for i, s := range mergable {
			if s != segments[i+index] {
				panic(fmt.Sprint("unexpected segment change,", s, segments[i+index]))
			}
		}

		for _, s := range mergable {
			s.removeOnFinalize()

			if err != nil {
				db.Unlock()
				return err
			}
		}

		newsegments := make([]segment, 0)

		newsegments = append(newsegments, segments[:index]...)
		newsegments = append(newsegments, newseg)
		newsegments = append(newsegments, segments[index+len(mergable):]...)

		db.segments = newsegments
		index++
		db.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func mergeSegments1(deleter Deleter, dbpath string, segments []segment) (segment, error) {

	lowerId := segments[0].LowerID()
	upperId := segments[len(segments)-1].UpperID()

	keyFilename := filepath.Join(dbpath, fmt.Sprintf("keys.%d.%d", lowerId, upperId))
	dataFilename := filepath.Join(dbpath, fmt.Sprintf("data.%d.%d", lowerId, upperId))

	files := make([]string, 0)
	for _, s := range segments {
		files = append(files, s.files()...)
	}

	ms := newMultiSegment(segments)
	itr, err := ms.Lookup(nil, nil)
	if err != nil {
		return nil, err
	}

	seg, err := writeAndLoadSegment(keyFilename, dataFilename, itr)
	if err != nil {
		return nil, err
	}
	err = deleter.scheduleDeletion(files)
	if err != nil {
		return nil, err
	}
	return seg, nil
}
