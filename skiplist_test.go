package leveldb

import "testing"

func compare(a int64, b int64) int {
	return int(a - b)
}

func TestSkipList_Contains(t *testing.T) {
	s := NewSkipList(compare)
	s.Put(1)
	s.Put(2)
	if !s.Contains(1) {
		t.Fail()
	}
	if !s.Contains(2) {
		t.Fail()
	}
	if s.Contains(3) {
		t.Fail()
	}
}

func BenchmarkSkipList_insert(b *testing.B) {
	s := NewSkipList(compare)
	for i := 0; i < b.N; i++ {
		s.Put(int64(i))
	}
}
