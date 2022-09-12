package leveldb

import (
	"bufio"
	"encoding/binary"
	"fmt"
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
	file    *os.File
	w       *bufio.Writer
	id      uint64
	inBatch bool
}

func newLogFile(path string, id uint64) (*logFile, error) {
	f, err := os.Create(filepath.Join(path, "log."+fmt.Sprint(id)))
	if err != nil {
		return nil, err
	}
	return &logFile{file: f, id: id, w: bufio.NewWriter(f)}, nil
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
	if !f.inBatch {
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

func readLogFile(path string) (*SkipList[KeyValue], error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := bufio.NewReader(f)
	list := NewSkipList(KeyValueCompare)

	for {
		var len int32
		var kLen, vLen int32
		err := binary.Read(r, binary.LittleEndian, &len)
		if err == io.EOF {
			return &list, nil
		}
		if len < 0 {
			// start of batch
			for i := 0; i < int(len*-1); i++ {
				err := binary.Read(r, binary.LittleEndian, &kLen)
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
			// read end of batch marker
			var len0 int32
			err := binary.Read(r, binary.LittleEndian, &len0)
			if err != nil {
				return nil, err
			}
			if len0 != len {
				return nil, DatabaseCorrupted
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
