package structkeys

import (
	"bytes"
	"fmt"
	"github.com/robaho/leveldb"
	"strings"
	"testing"
	"time"
)

// demonstrate the use of structures as keys

type MyKey struct {
	Symbol string
	At     time.Time
}

func (mk MyKey) MarshalBinary() ([]byte, error) {
	// A simple encoding: plain text.
	var b bytes.Buffer
	fmt.Fprintln(&b, mk.Symbol, mk.At)
	return b.Bytes(), nil
}

// UnmarshalBinary modifies the receiver so it must take a pointer receiver.
func (mk *MyKey) UnmarshalBinary(data []byte) error {
	// A simple encoding: plain text.
	b := bytes.NewBuffer(data)
	_, err := fmt.Fscanln(b, &mk.Symbol, &mk.At)
	return err
}

func (*MyKey) Less(a []byte, b []byte) bool {
	_a := MyKey{}
	_b := MyKey{}

	_a.UnmarshalBinary(a)
	_b.UnmarshalBinary(b)

	c := strings.Compare(_a.Symbol, _b.Symbol)
	if c == 0 {
		return _a.At.Before(_b.At)
	}
	return c < 0
}

func TestCustomKeys(t *testing.T) {

	path := "test/structkeys"

	leveldb.Remove(path)
	db, err := leveldb.Open(path, leveldb.Options{CreateIfNeeded: true})
	if err != nil {
		panic(err)
	}

	a := MyKey{"ibm", time.Now()}
	b := MyKey{"aapl", time.Now()}

	_a, _ := a.MarshalBinary()
	_b, _ := b.MarshalBinary()

	db.Put(_a, []byte("some value for a"))
	db.Put(_b, []byte("some value for b"))

	value, err := db.Get(_a)
	if string(value) != "some value for a" {
		panic("wrong a value")
	}

	itr, err := db.Lookup(nil, nil)
	if err != nil {
		panic(err)
	}
	key, value, err := itr.Next()

	mykey := MyKey{}
	mykey.UnmarshalBinary(key)

	if mykey.Symbol != "aapl" {
		panic("wrong key")
	}

	key, value, err = itr.Next()
	mykey.UnmarshalBinary(key)
	if mykey.Symbol != "ibm" {
		panic("wrong key")
	}

	db.Close()
}
