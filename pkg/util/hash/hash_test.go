package hash

import (
	"net"
	"testing"
)

func TestNewSession(t *testing.T) {
	for i := 0; i < 100; i++ {
		s := NewSession(i)
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

func TestGetMacAddress(t *testing.T) {
	mac, err := GetMacAddress()
	if err != nil {
		t.Log("Could not find mac. err=", err)
		return
	}

	ms, _ := net.ParseMAC(string(net.HardwareAddr(mac).String()))
	t.Log("mac= ", ms, " ", mac)
}
