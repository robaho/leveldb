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
		if atomic.LoadInt32(&db.closing) > 0 || db.err != nil {
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

func mergeSegments0(db *Database, segmentCount uint) error {
	// only a single routine can be in mergeSegments0 to avoid deadlock
	if !atomic.CompareAndSwapInt32(&db.inMerge, 0, 1) {
		return nil
	}
	defer atomic.StoreInt32(&db.inMerge, 0)

	//fmt.Println("merging segments", db.path)

	for {

		segments := db.getState().segments

		if len(segments) <= int(segmentCount) {
			return nil
		}

		maxMergeSize := len(segments) / 2
		if maxMergeSize < 4 {
			maxMergeSize = 4
		}

		// ensure that only valid disk segments are merged

		mergable := make([]segment, 0)

		smallest := 0
		for i, s := range segments[1:] {
			if s.size() < segments[smallest].size() {
				smallest = i
			}
		}

		if smallest > 0 && smallest == len(segments)-1 {
			smallest--
		}

		index := smallest

		for _, s := range segments[index:] {
			mergable = append(mergable, s)
			if len(mergable) == maxMergeSize {
				break
			}
		}

		segments = segments[index : index+len(mergable)]

		newseg, err := mergeSegments1(db.deleter, db.path, segments, index == 0)
		if err != nil {
			return err
		}

		db.Lock() // need lock when updating db segments
		segments = db.state.segments

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

		db.setState(&dbState{segments: newsegments, memory: db.state.memory, multi: newMultiSegment(copyAndAppend(newsegments, db.state.memory))})
		index++
		db.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func mergeSegments1(deleter Deleter, dbpath string, segments []segment, purgeDeleted bool) (segment, error) {

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

	seg, err := writeAndLoadSegment(keyFilename, dataFilename, itr, purgeDeleted)
	if err != nil {
		return nil, err
	}
	err = deleter.scheduleDeletion(files)
	if err != nil {
		return nil, err
	}
	return seg, nil
}
