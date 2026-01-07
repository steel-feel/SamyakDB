package storage

import (
	"bytes"
	"testing"
)

func TestStorage(t *testing.T) {
	s := NewStorage()

	// Test Put and Get
	s.Put("key1", []byte("value1"))
	val, found := s.Get("key1")
	if !found {
		t.Errorf("expected key1 to be found")
	}
	if !bytes.Equal(val, []byte("value1")) {
		t.Errorf("expected value1, got %s", string(val))
	}

	// Test Update
	s.Put("key1", []byte("value2"))
	val, found = s.Get("key1")
	if !found {
		t.Errorf("expected key1 to be found")
	}
	if !bytes.Equal(val, []byte("value2")) {
		t.Errorf("expected value2, got %s", string(val))
	}

	// Test Delete
	success := s.Delete("key1")
	if !success {
		t.Errorf("expected delete to be successful")
	}
	_, found = s.Get("key1")
	if found {
		t.Errorf("expected key1 to be deleted")
	}

	// Test Delete non-existent
	success = s.Delete("key2")
	if success {
		t.Errorf("expected delete of non-existent key to return false")
	}
}
