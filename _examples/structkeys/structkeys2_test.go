package structkeys

import (
	"github.com/robaho/leveldb"
	"testing"
	"time"
)

type MyKeyDB struct {
	db *leveldb.Database
}

func (mydb *MyKeyDB) Get(key MyKey) (string, error) {
	_key, err := key.MarshalBinary()
	if err != nil {
		return "", err
	}

	val, err := mydb.db.Get(_key)
	return string(val), err
}
func (mydb *MyKeyDB) Put(key MyKey, value string) error {
	_key, err := key.MarshalBinary()
	if err != nil {
		return err
	}
	return mydb.db.Put(_key, []byte(value))
}

func TestCustomKeys2(t *testing.T) {

	path := "test/structkeys"

	leveldb.Remove(path)
	db, err := leveldb.Open(path, leveldb.Options{CreateIfNeeded: true})
	if err != nil {
		panic(err)
	}

	mydb := MyKeyDB{db}

	a := MyKey{"ibm", time.Now()}
	b := MyKey{"aapl", time.Now()}

	mydb.Put(a, "some value for a")
	mydb.Put(b, "some value for b")

	value, err := mydb.Get(a)
	if value != "some value for a" {
		panic("wrong a value")
	}
	value, err = mydb.Get(b)
	if value != "some value for b" {
		panic("wrong a value")
	}

	db.Close()
}
