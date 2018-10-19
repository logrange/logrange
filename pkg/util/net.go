package util

import (
	"fmt"
	"net"
	"strings"
)

// GetMacAddress returns a non-loopback interface MAC address. It returns an
// error with the reason, if it is not possible to discover one.
func GetMacAddress() ([]byte, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}

	var ip string
	for _, a := range addrs {
		if ipn, ok := a.(*net.IPNet); ok && !ipn.IP.IsLoopback() {
			if ipn.IP.To4() != nil {
				ip = ipn.IP.String()
				break
			}
		}
	}
	if ip == "" {
		return nil, fmt.Errorf("could not find any ip address except loopback")
	}

	ifss, err := net.Interfaces()
	if err != nil {
		return nil, err
	}

	for _, ifs := range ifss {
		if addrs, err := ifs.Addrs(); err == nil {
			for _, addr := range addrs {
				if strings.Contains(addr.String(), ip) {
					nif, err := net.InterfaceByName(ifs.Name)
					if err != nil {
						continue
					}

					return []byte(nif.HardwareAddr), nil
				}
			}
		}
	}
	return nil, fmt.Errorf("could not find any interface with MAC address")
}
