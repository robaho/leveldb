package leveldb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
)

// diskSegment is a read-only immutable portion of the database.
//
// The key file uses 4096 byte blocks, the format is
//
//	keylen uint16
//	key []byte
//	dataoffset int64
//	datalen uint32 (if datalen is 0, the key is "removed")
//
// keylen supports compressed keys. if the high bit is set, then the key is compressed,
// with the 8 lower bits for the key len, and the next 7 bits for the run length. a block
// will never start with a compressed key
//
// the special value of 0x7000 marks the end of a block
//
// the data file can only be read in conjunction with the key
// file since there is no length attribute, it is a raw appended
// byte array with the offset and length in the key file
//
// The filenames are prefix.lower.upper, where prefix is 'keys' or 'data', and lower/upper is the
// segment identifier range contained in the file. Invalid filenames in the database will cause
// a panic on open.
type diskSegment struct {
	keyFile   *memoryMappedFile
	keyBlocks int64
	dataFile  *memoryMappedFile
	lowerID   uint64
	upperID   uint64
	// nil for segments loaded during initial open
	// otherwise holds the key for every keyIndexInterval block
	keyIndex [][]byte
	filesize uint64
}

type diskSegmentIterator struct {
	segment      *diskSegment
	lower        []byte
	upper        []byte
	buffer       []byte
	block        int64
	bufferOffset int
	key          []byte
	data         []byte
	isValid      bool
	err          error
	finished     bool
}

func loadDiskSegments(directory string, options Options) ([]segment, error) {
	files, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	segments := []segment{}
	// first remove any 'tmp' files and related non-temp files as this signifies
	// a failure during write
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".tmp") {
			continue
		}
		base := strings.TrimSuffix(file.Name(), ".tmp")
		var segs string
		if strings.HasPrefix(base, "keys.") {
			segs = strings.TrimPrefix(base, "keys.")
		} else {
			segs = strings.TrimPrefix(base, "data.")
		}
		removeFileIfExists := func(filename string) error {
			err := os.Remove(filename)
			if err != nil && !os.IsNotExist(err) {
				return err
			}
			return nil
		}
		err0 := removeFileIfExists(fmt.Sprint("keys.", segs))
		err1 := removeFileIfExists(fmt.Sprint("data.", segs))
		err2 := removeFileIfExists(fmt.Sprint("keys.", segs, ".tmp"))
		err3 := removeFileIfExists(fmt.Sprint("data.", segs, ".tmp"))
		err = errn(err0, err1, err2, err3)
		if err != nil {
			return nil, err
		}
	}
	// re-read as temporary files should be removed
	files, err = os.ReadDir(directory)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if strings.HasPrefix(file.Name(), "log.") {
			ls, err := newLogSegment(filepath.Join(directory, file.Name()), options)
			if err != nil {
				panic(fmt.Sprint("unable to load logSegment", file, err))
			}
			segments = append(segments, ls)
			continue
		}
		if !strings.HasPrefix(file.Name(), "keys.") {
			continue
		}
		lowerId, upperId := getSegmentIDs(file.Name())
		keyFilename := filepath.Join(directory, fmt.Sprintf("keys.%d.%d", lowerId, upperId))
		dataFilename := filepath.Join(directory, fmt.Sprintf("data.%d.%d", lowerId, upperId))
		segment, err := newDiskSegment(keyFilename, dataFilename, nil)
		if err != nil {
			return nil, err
		}
		segments = append(segments, segment) // don't have keyIndex
	}
	sort.Slice(segments, func(i, j int) bool {
		id1, id2 := segments[i].UpperID(), segments[j].UpperID()
		if id1 == id2 {
			// the only way this is possible is if we have a log file that has already been merged, but
			// wasn't deleted, so sort the log file first
			return segments[i].LowerID() > segments[j].LowerID()
		}
		return id1 < id2
	})
	// remove any segments that are fully contained in another segment
next:
	for i := 0; i < len(segments); {
		seg := segments[i]
		for j := i + 1; j < len(segments); j++ {
			seg0 := segments[j]
			if seg.LowerID() >= seg0.LowerID() && seg.UpperID() <= seg0.UpperID() {
				segments = append(segments[:i], segments[i+1:]...)
				seg.removeSegment()
				continue next
			}
		}
		i++
	}
	return segments, nil
}

func getSegmentID(filename string) (id uint64) {
	base := filepath.Base(filename)
	segs := strings.Split(base, ".")
	id0, err := strconv.Atoi(segs[1])
	if err != nil {
		panic(fmt.Sprint("invalid segment filename", base))
	}
	return uint64(id0)
}

func getSegmentIDs(filename string) (lower, upper uint64) {
	base := filepath.Base(filename)
	segs := strings.Split(base, ".")
	id0, err := strconv.Atoi(segs[1])
	if err != nil {
		panic(fmt.Sprint("invalid segment filename", base))
	}
	id1, err := strconv.Atoi(segs[2])
	if err != nil {
		panic(fmt.Sprint("invalid segment filename", base))
	}
	return uint64(id0), uint64(id1)
}

func newDiskSegment(keyFilename, dataFilename string, keyIndex [][]byte) (segment, error) {

	lower, upper := getSegmentIDs(keyFilename)

	ds := &diskSegment{}
	kf, err := newMemoryMappedFile(keyFilename)
	if err != nil {
		panic(err)
	}
	df, err := newMemoryMappedFile(dataFilename)
	if err != nil {
		panic(err)
	}
	ds.keyFile = kf
	ds.dataFile = df
	ds.lowerID = lower
	ds.upperID = upper

	ds.keyBlocks = (kf.Length()-1)/keyBlockSize + 1

	if keyIndex == nil {
		// TODO maybe load this in the background
		keyIndex = loadKeyIndex(kf, ds.keyBlocks)
	}

	ds.keyIndex = keyIndex
	kInfo, err := os.Stat(keyFilename)
	if err != nil {
		return nil, err
	}
	dInfo, err := os.Stat(dataFilename)
	if err != nil {
		return nil, err
	}
	ds.filesize = uint64(kInfo.Size() + dInfo.Size())
	return ds, nil
}

func (ds *diskSegment) size() uint64 {
	return ds.filesize
}

func loadKeyIndex(kf *memoryMappedFile, keyBlocks int64) [][]byte {
	buffer := make([]byte, keyBlockSize)
	keyIndex := make([][]byte, 0)

	if kf.Length() == 0 {
		return keyIndex
	}

	var block int64
	for block = 0; block < keyBlocks; block += int64(keyIndexInterval) {
		_, err := kf.ReadAt(buffer, block*keyBlockSize)
		if err != nil {
			keyIndex = nil
			break
		}
		keylen := binary.LittleEndian.Uint16(buffer)
		if keylen == endOfBlock {
			break
		}
		keycopy := make([]byte, keylen)
		copy(keycopy, buffer[2:2+keylen])
		keyIndex = append(keyIndex, keycopy)
	}
	return keyIndex
}

func (dsi *diskSegmentIterator) Next() (key []byte, value []byte, err error) {
	if dsi.isValid {
		dsi.isValid = false
		return dsi.key, dsi.data, dsi.err
	}
	dsi.nextKeyValue()
	dsi.isValid = false
	return dsi.key, dsi.data, dsi.err
}

func (dsi *diskSegmentIterator) peekKey() ([]byte, error) {
	if dsi.isValid {
		return dsi.key, dsi.err
	}
	dsi.nextKeyValue()
	return dsi.key, dsi.err
}

func (dsi *diskSegmentIterator) nextKeyValue() error {
	if dsi.finished {
		return EndOfIterator
	}
	var prevKey = dsi.key

	for {
		keylen := binary.LittleEndian.Uint16(dsi.buffer[dsi.bufferOffset:])
		if keylen == endOfBlock {
			dsi.block++
			if dsi.block == dsi.segment.keyBlocks {
				dsi.finished = true
				dsi.err = EndOfIterator
				dsi.key = nil
				dsi.data = nil
				dsi.isValid = true
				return dsi.err
			}
			n, err := dsi.segment.keyFile.ReadAt(dsi.buffer, dsi.block*keyBlockSize)
			if err != nil {
				return err
			}
			if n != keyBlockSize {
				return errors.New(fmt.Sprint("did not read block size, read ", n))
			}
			dsi.bufferOffset = 0
			prevKey = nil
			continue
		}
		prefixLen, compressedLen, err := decodeKeyLen(keylen)
		if err != nil {
			return err
		}

		dsi.bufferOffset += 2
		key := dsi.buffer[dsi.bufferOffset : dsi.bufferOffset+int(compressedLen)]
		dsi.bufferOffset += int(compressedLen)

		key = decodeKey(key, prevKey, prefixLen)

		dataoffset := binary.LittleEndian.Uint64(dsi.buffer[dsi.bufferOffset:])
		dsi.bufferOffset += 8
		datalen := binary.LittleEndian.Uint32(dsi.buffer[dsi.bufferOffset:])
		dsi.bufferOffset += 4

		prevKey = key

		if dsi.lower != nil {
			if less(key, dsi.lower) {
				continue
			}
			if equal(key, dsi.lower) {
				goto found
			}
		}
		if dsi.upper != nil {
			if equal(key, dsi.upper) {
				goto found
			}
			if !less(key, dsi.upper) {
				dsi.finished = true
				dsi.isValid = true
				dsi.key = nil
				dsi.data = nil
				dsi.err = EndOfIterator
				return EndOfIterator
			}
		}
	found:

		if datalen == 0 {
			dsi.data = emptyBytes
		} else {
			dsi.data = make([]byte, datalen)
			_, err = dsi.segment.dataFile.ReadAt(dsi.data, int64(dataoffset))
		}
		dsi.key = key
		dsi.isValid = true
		return err
	}
}

func (ds *diskSegment) LowerID() uint64 {
	return ds.lowerID
}

func (ds *diskSegment) UpperID() uint64 {
	return ds.upperID
}

func (ds *diskSegment) Put(key []byte, value []byte) ([]byte, error) {
	panic("disk segments are immutable, unable to Put")
}

func (ds *diskSegment) Remove(key []byte) ([]byte, error) {
	panic("disk segments are immutable, unable to Remove")
}

var emptyBytes = make([]byte, 0)

func (ds *diskSegment) Get(key []byte) ([]byte, error) {
	offset, len, err := binarySearch(ds, key)
	if err != nil {
		return nil, err
	}

	if len == 0 {
		return emptyBytes, nil
	}

	buffer := make([]byte, len)
	_, err = ds.dataFile.ReadAt(buffer, offset)
	if err != nil {
		return nil, err
	}
	return buffer, nil
}

func binarySearch(ds *diskSegment, key []byte) (offset int64, length uint32, err error) {
	var buffer [maxKeySize]byte

	// use memory index to narrow search
	index := sort.Search(len(ds.keyIndex), func(i int) bool {
		return less(key, ds.keyIndex[i])
	})

	if index == 0 {
		return 0, 0, KeyNotFound
	}

	index--

	var lowblock = int64(index * keyIndexInterval)
	var highblock = lowblock + int64(keyIndexInterval)

	if highblock >= ds.keyBlocks {
		highblock = ds.keyBlocks - 1
	}

	block, err := binarySearch0(ds, lowblock, highblock, key, buffer[:])
	if err != nil {
		return 0, 0, err
	}
	return scanBlock(ds, block, key)
}

// returns the block that may contain the key, or possible the next block - since we do not have a 'last key' of the block
func binarySearch0(ds *diskSegment, lowBlock int64, highBlock int64, key []byte, buffer []byte) (int64, error) {
	if highBlock-lowBlock <= 1 {
		// the key is either in low block or high block, or does not exist, so check high block
		ds.keyFile.ReadAt(buffer, highBlock*keyBlockSize)
		keylen := binary.LittleEndian.Uint16(buffer)
		skey := buffer[2 : 2+keylen]
		if less(key, skey) {
			return lowBlock, nil
		} else {
			return highBlock, nil
		}
	}

	block := (highBlock-lowBlock)/2 + lowBlock

	ds.keyFile.ReadAt(buffer, block*keyBlockSize)
	keylen := binary.LittleEndian.Uint16(buffer)
	skey := buffer[2 : 2+keylen]

	if less(key, skey) {
		return binarySearch0(ds, lowBlock, block, key, buffer)
	} else {
		return binarySearch0(ds, block, highBlock, key, buffer)
	}
}

func scanBlock(ds *diskSegment, block int64, key []byte) (offset int64, len uint32, err error) {
	var buffer [keyBlockSize]byte

	_, err = ds.keyFile.ReadAt(buffer[:], block*keyBlockSize)
	if err != nil {
		return 0, 0, err
	}

	index := 0
	var prevKey []byte = nil
	for {
		keylen := binary.LittleEndian.Uint16(buffer[index:])
		if keylen == endOfBlock {
			return 0, 0, KeyNotFound
		}

		var compressedLen = keylen
		var prefixLen = 0

		if keylen&compressedBit != 0 {
			prefixLen = int((keylen >> 8) & maxPrefixLen)
			compressedLen = keylen & maxCompressedLen
		}

		endkey := index + 2 + int(compressedLen)
		_key := buffer[index+2 : endkey]

		if prefixLen > 0 {
			_key = append(prevKey[:prefixLen], _key...)
		}

		prevKey = _key

		if bytes.Equal(_key, key) {
			offset = int64(binary.LittleEndian.Uint64(buffer[endkey:]))
			len = binary.LittleEndian.Uint32(buffer[endkey+8:])
			return
		}
		if !less(_key, key) {
			return 0, 0, KeyNotFound
		}
		index = endkey + 12
	}
}

func (ds *diskSegment) Lookup(lower []byte, upper []byte) (LookupIterator, error) {
	if ds.keyFile.Length() == 0 {
		return &emptyIterator{}, nil
	}
	buffer := make([]byte, keyBlockSize)
	var block int64 = 0
	if lower != nil {
		index := sort.Search(len(ds.keyIndex), func(i int) bool {
			return less(lower, ds.keyIndex[i])
		})
		index--
		if index < 0 {
			index = 0
		}
		block = int64(index * keyIndexInterval)
	}
	n, err := ds.keyFile.ReadAt(buffer, block*keyBlockSize)
	if err != nil {
		return nil, err
	}
	if n != keyBlockSize {
		return nil, errors.New(fmt.Sprint("did not read block size ", n))
	}
	return &diskSegmentIterator{segment: ds, lower: lower, upper: upper, buffer: buffer, block: block}, nil
}

func (ds *diskSegment) Close() error {
	err0 := ds.keyFile.Close()
	err1 := ds.dataFile.Close()
	return errn(err0, err1)
}

func (ds *diskSegment) removeSegment() error {
	err0 := ds.Close()
	err1 := os.Remove(ds.keyFile.Name())
	err2 := os.Remove(ds.dataFile.Name())
	return errn(err0, err1, err2)
}
func (ds *diskSegment) removeOnFinalize() {
	//fmt.Println("scheduled ", ds.keyFile.Name(), "for deletion")
	runtime.SetFinalizer(ds, func(ds *diskSegment) { ds.removeSegment() })
}
func (ds *diskSegment) files() []string {
	return []string{filepath.Base(ds.keyFile.Name()), filepath.Base(ds.dataFile.Name())}
}
