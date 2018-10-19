package util

import "testing"

func TestNewSessionId(t *testing.T) {
	for i := 0; i < 100; i++ {
		s := NewSessionId(i)
		if len(s) != i {
			t.Fatal("Unexpected length ", len(s), ", but wanted ", i)
		}
		t.Log(s)
	}
}

func TestNewPassword(t *testing.T) {
	for i := 0; i < 100; i++ {
		s := NewPassword(i)
		if len(s) != i {
			t.Fatal("Unexpected length ", len(s), ", but wanted ", i)
		}
		t.Log(s)
	}
}
