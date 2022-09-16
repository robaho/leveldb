package leveldb

import (
	"bytes"
	"errors"
	"os"
	"testing"
)

func writeLogFile() error {
	lf, err := newLogFile("test", 0, Options{})
	if err != nil {
		return err
	}
	err = lf.Write([]byte("mykey"), []byte("myvalue"))
	if err != nil {
		return err
	}
	err = lf.StartBatch(2)
	if err != nil {
		return err
	}
	err = lf.Write([]byte("batchkey1"), []byte("batchvalue1"))
	if err != nil {
		return err
	}
	err = lf.Write([]byte("batchkey2"), []byte("batchvalue2"))
	if err != nil {
		return err
	}
	err = lf.EndBatch(2)
	if err != nil {
		return err
	}
	err = lf.Close()
	if err != nil {
		return err
	}
	return nil
}

func testKeyValue(s *SkipList[KeyValue], key string, value string) error {
	r, ok := s.get(KeyValue{key: []byte(key)})
	if !ok {
		return errors.New("key not found")
	}
	if bytes.Compare([]byte(value), r.value) != 0 {
		return errors.New("incorrect value")
	}
	return nil
}

func TestLogFile_Write(t *testing.T) {
	err := writeLogFile()
	if err != nil {
		t.Fatal(err)
	}
	s, err := readLogFile("test/log.0", Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err = testKeyValue(s, "mykey", "myvalue"); err != nil {
		t.Fatal(err)
	}
	if err = testKeyValue(s, "batchkey1", "batchvalue1"); err != nil {
		t.Fatal(err)
	}
	if err = testKeyValue(s, "batchkey2", "batchvalue2"); err != nil {
		t.Fatal(err)
	}
}

func TestLogFile_InvalidFile(t *testing.T) {
	err := writeLogFile()
	if err != nil {
		t.Fatal(err)
	}
	err = os.Truncate("test/log.0", 1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = readLogFile("test/log.0", Options{})
	if err == nil {
		t.Fatal("file should have failed to load", err)
	}
}

func TestLogFile_PartialBatch(t *testing.T) {
	err := writeLogFile()
	if err != nil {
		t.Fatal(err)
	}
	err = os.Truncate("test/log.0", 84-12) // truncate into batch
	if err != nil {
		t.Fatal(err)
	}
	_, err = readLogFile("test/log.0", Options{BatchReadMode: ReturnOpenError})
	if err == nil {
		t.Fatal("file should have failed to load", err)
	}
	s, err := readLogFile("test/log.0", Options{BatchReadMode: DiscardPartial})
	if err != nil {
		t.Fatal("file should have opened", err)
	}
	if err = testKeyValue(s, "mykey", "myvalue"); err != nil {
		t.Fatal(err)
	}
	if err = testKeyValue(s, "batchkey1", "batchvalue1"); err == nil {
		t.Fatal("batchkey1 should have been dropped")
	}
	if err = testKeyValue(s, "batchkey2", "batchvalue2"); err == nil {
		t.Fatal("batchkey2 should have been dropped")
	}
	s, err = readLogFile("test/log.0", Options{BatchReadMode: ApplyPartial})
	if err != nil {
		t.Fatal("file should have opened", err)
	}
	if err = testKeyValue(s, "mykey", "myvalue"); err != nil {
		t.Fatal(err)
	}
	if err = testKeyValue(s, "batchkey1", "batchvalue1"); err != nil {
		t.Fatal("batchkey1 should have been found")
	}
	if err = testKeyValue(s, "batchkey2", "batchvalue2"); err == nil {
		t.Fatal("batchkey2 should have been dropped")
	}
}
