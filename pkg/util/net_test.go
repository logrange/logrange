package util

import (
	"net"
	"testing"
)

func TestGetMacAddress(t *testing.T) {
	mac, err := GetMacAddress()
	if err != nil {
		t.Log("Could not find mac. err=", err)
		return
	}

	ms, _ := net.ParseMAC(string(net.HardwareAddr(mac).String()))
	t.Log("mac= ", ms, " ", mac)
}
