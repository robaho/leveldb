package leveldb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/robaho/leveldb/skip"
	"io"
	"os"
	"path/filepath"
)

// The log file format is:
//
//	StartBatchMarker is { negative int32 length of batch }
//	EndBatchMarker is { negative int32 length of batch which matches StartBatchMarker }
//	LogEntry is { int32 key len, key bytes, int32 value len, value bytes }
type logFile struct {
	file         *os.File
	w            *bufio.Writer
	id           uint64
	inBatch      bool
	syncWrite    bool
	disableFlush bool
}

func newLogFile(path string, id uint64, options Options) (*logFile, error) {
	mode := os.O_TRUNC | os.O_WRONLY | os.O_CREATE
	if options.EnableSyncWrite {
		mode = mode | os.O_SYNC
	}
	f, err := os.OpenFile(filepath.Join(path, "log."+fmt.Sprint(id)), mode, 0644)
	if err != nil {
		return nil, err
	}
	l := logFile{file: f, id: id, w: bufio.NewWriter(f)}
	if !options.EnableSyncWrite && options.DisableWriteFlush {
		l.disableFlush = true
	}
	return &l, nil
}
func (f *logFile) StartBatch(len int) error {
	f.inBatch = true
	return binary.Write(f.w, binary.LittleEndian, int32(-len))
}
func (f *logFile) EndBatch(len int) error {
	f.inBatch = false
	err := binary.Write(f.w, binary.LittleEndian, int32(-len))
	if err != nil {
		return err
	}
	return f.w.Flush()
}
func (f *logFile) Write(key []byte, value []byte) error {
	err := binary.Write(f.w, binary.LittleEndian, int32(len(key)))
	if err != nil {
		return err
	}
	err = binary.Write(f.w, binary.LittleEndian, key)
	if err != nil {
		return err
	}
	err = binary.Write(f.w, binary.LittleEndian, int32(len(value)))
	if err != nil {
		return err
	}
	err = binary.Write(f.w, binary.LittleEndian, value)
	if err != nil {
		return err
	}
	if !f.inBatch && !f.disableFlush {
		return f.w.Flush()
	}
	return nil
}

func (f *logFile) Close() error {
	f.w.Flush()
	return f.file.Close()
}

func (f *logFile) Remove() error {
	return os.Remove(f.file.Name())
}

func keyValueCompare(options Options) func(a, b KeyValue) int {
	if options.UserKeyCompare == nil {
		return func(a, b KeyValue) int {
			return bytes.Compare(a.key, b.key)
		}
	} else {
		return func(a, b KeyValue) int {
			return options.UserKeyCompare(a.key, b.key)
		}
	}
}

func readLogFile(path string, options Options) (*skip.SkipList[KeyValue], error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := bufio.NewReader(f)

	list := skip.NewSkipList(keyValueCompare(options))

	var len, kLen, vLen int32

	readBatch := func(len int32) error {
		var err error
		var len0 int32
		entries := make([]KeyValue, 0)
		// start of batch
		for i := 0; i < int(len*-1); i++ {
			err = binary.Read(r, binary.LittleEndian, &kLen)
			if err != nil {
				goto batchReadError
			}
			key := make([]byte, kLen)
			err = binary.Read(r, binary.LittleEndian, &key)
			if err != nil {
				goto batchReadError
			}
			err = binary.Read(r, binary.LittleEndian, &vLen)
			if err != nil {
				goto batchReadError
			}
			value := make([]byte, vLen)
			err = binary.Read(r, binary.LittleEndian, &value)
			if err != nil {
				goto batchReadError
			}
			entries = append(entries, KeyValue{key: key, value: value})
		}
		// read end of batch marker
		err = binary.Read(r, binary.LittleEndian, &len0)
		if err != nil {
			goto batchReadError
		}
		if len0 != len {
			err = DatabaseCorrupted
			goto batchReadError
		}
	batchReadError:
		if options.BatchReadMode == ApplyPartial || err == nil {
			for _, e := range entries {
				list.Put(e)
			}
		}
		return err
	}

	for {
		err := binary.Read(r, binary.LittleEndian, &len)
		if err == io.EOF {
			return &list, nil
		}
		if err != nil {
			return nil, err
		}
		if len < 0 {
			err = readBatch(len)
			if err != nil {
				if options.BatchReadMode == ReturnOpenError {
					return nil, err
				}
				return &list, nil
			}
		} else {
			kLen = len
			key := make([]byte, kLen)
			err = binary.Read(r, binary.LittleEndian, &key)
			if err != nil {
				return nil, err
			}
			err = binary.Read(r, binary.LittleEndian, &vLen)
			if err != nil {
				return nil, err
			}
			value := make([]byte, vLen)
			err = binary.Read(r, binary.LittleEndian, &value)
			if err != nil {
				return nil, err
			}
			list.Put(KeyValue{key: key, value: value})
		}
	}
	return &list, nil
}
