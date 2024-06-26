package leveldb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const keyBlockSize = 4096
const maxKeySize = 1000
const endOfBlock uint16 = 0x8000
const compressedBit uint16 = 0x8000
const maxPrefixLen uint16 = 0xFF ^ 0x80
const maxCompressedLen uint16 = 0xFF
const keyIndexInterval int = 16

// called to write a memory segment to disk after which the memory segment is closed, and the log file removed
func writeSegmentToDisk(db *Database, seg *memorySegment) error {
	itr, err := seg.Lookup(nil, nil)
	if err != nil {
		return err
	}

	if _, err = itr.peekKey(); err == EndOfIterator {
		seg.removeSegment()
		// simply return and re-use existing memory segment
		return nil
	}

	lowerId := seg.LowerID()
	upperId := seg.LowerID()

	keyFilename := filepath.Join(db.path, fmt.Sprintf("keys.%d.%d", lowerId, upperId))
	dataFilename := filepath.Join(db.path, fmt.Sprintf("data.%d.%d", lowerId, upperId))

	_, err = writeAndLoadSegment(keyFilename, dataFilename, itr, false)
	if err != nil {
		return err
	}
	seg.removeSegment()

	return nil
}

func writeAndLoadSegment(keyFilename, dataFilename string, itr LookupIterator, purgeDeleted bool) (segment, error) {

	_, err := os.Stat(keyFilename);
	if(err==nil || !os.IsNotExist(err)) {
		return nil,err;
	}
	_, err = os.Stat(dataFilename);
	if(err==nil || !os.IsNotExist(err)) {
		return nil,err;
	}

	keyFilenameTmp := keyFilename + ".tmp"
	dataFilenameTmp := dataFilename + ".tmp"

	keyIndex, err := writeSegmentFiles(keyFilenameTmp, dataFilenameTmp, itr, purgeDeleted)
	if err != nil {
		os.Remove(keyFilenameTmp)
		os.Remove(dataFilenameTmp)
		return nil, err
	}

	os.Rename(keyFilenameTmp, keyFilename)
	os.Rename(dataFilenameTmp, dataFilename)

	return newDiskSegment(keyFilename, dataFilename, keyIndex)
}

func writeSegmentFiles(keyFName, dataFName string, itr LookupIterator, purgeDeleted bool) ([][]byte, error) {

	var keyIndex [][]byte

	keyF, err := os.OpenFile(keyFName, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}
	defer keyF.Close()

	dataF, err := os.OpenFile(dataFName, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}
	defer dataF.Close()

	keyW := bufio.NewWriter(keyF)
	dataW := bufio.NewWriter(dataF)

	var dataOffset int64
	var keyBlockLen int
	var keyCount = 0
	var block = 0

	var zeros = make([]byte, keyBlockSize)

	var prevKey []byte

	for {
		key, value, err := itr.Next()
		if err != nil {
			break
		}
		if purgeDeleted && len(value) == 0 {
			continue
		}
		keyCount++

		dataW.Write(value)
		if keyBlockLen+2+len(key)+8+4 >= keyBlockSize-2 { // need to leave room for 'end of block marker'
			// key won't fit in block so move to next
			binary.Write(keyW, binary.LittleEndian, endOfBlock)
			keyBlockLen += 2
			keyW.Write(zeros[:keyBlockSize-keyBlockLen])
			keyBlockLen = 0
			prevKey = nil
		}

		if keyBlockLen == 0 {
			if block%keyIndexInterval == 0 {
				keycopy := make([]byte, len(key))
				copy(keycopy, key)
				keyIndex = append(keyIndex, keycopy)
			}
			block++
		}

		dataLen := uint32(len(value))

		dk := encodeKey(key, prevKey)
		prevKey = make([]byte, len(key))
		copy(prevKey, key)

		var data = []interface{}{
			uint16(dk.keylen),
			dk.compressedKey,
			int64(dataOffset),
			uint32(dataLen)}
		buf := new(bytes.Buffer)
		for _, v := range data {
			err = binary.Write(buf, binary.LittleEndian, v)
			if err != nil {
				goto failed
			}
		}
		keyBlockLen += 2 + len(dk.compressedKey) + 8 + 4
		keyW.Write(buf.Bytes())
		if value != nil {
			dataOffset += int64(dataLen)
		}
	}

	// pad key file to block size
	if keyBlockLen > 0 && keyBlockLen < keyBlockSize {
		// key won't fit in block so move to next
		binary.Write(keyW, binary.LittleEndian, endOfBlock)
		keyBlockLen += 2
		keyW.Write(zeros[:keyBlockSize-keyBlockLen])
		keyBlockLen = 0
	}

	keyW.Flush()
	dataW.Flush()

	return keyIndex, nil

failed:
	return nil, err
}

type diskkey struct {
	keylen        uint16
	compressedKey []byte
}

func encodeKey(key, prevKey []byte) diskkey {

	prefixLen := calculatePrefixLen(prevKey, key)
	if prefixLen > 0 {
		key = key[prefixLen:]
		return diskkey{keylen: compressedBit | (uint16(prefixLen<<8) | uint16(len(key))), compressedKey: key}
	}
	return diskkey{keylen: uint16(len(key)), compressedKey: key}
}

func decodeKeyLen(keylen uint16) (prefixLen, compressedLen uint16, err error) {
	if (keylen & compressedBit) != 0 {
		prefixLen = (keylen >> 8) & maxPrefixLen
		compressedLen = keylen & maxCompressedLen
		if prefixLen > maxPrefixLen || compressedLen > maxCompressedLen {
			return 0, 0, errors.New(fmt.Sprint("invalid prefix/compressed length,", prefixLen, compressedLen))
		}
	} else {
		if keylen > maxKeySize {
			return 0, 0, errors.New(fmt.Sprint("key > 1024"))
		}
		compressedLen = keylen
	}
	if compressedLen == 0 {
		return 0, 0, errors.New("decoded key length is 0")
	}
	return
}

func decodeKey(key, prevKey []byte, prefixLen uint16) []byte {
	if prefixLen != 0 {
		key = append(prevKey[:prefixLen], key...)
	}
	return key
}

func calculatePrefixLen(prevKey []byte, key []byte) int {
	if prevKey == nil {
		return 0
	}
	var length = 0
	for ; length < len(prevKey) && length < len(key); length++ {
		if prevKey[length] != key[length] {
			break
		}
	}
	if length > int(maxPrefixLen) || len(key)-length > int(maxCompressedLen) {
		length = 0
	}
	return length
}
