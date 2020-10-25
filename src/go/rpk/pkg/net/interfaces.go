package net

import (
	"net"
	"vectorized/pkg/utils"

	log "github.com/sirupsen/logrus"
)

func GetInterfacesByIps(addresses ...string) ([]string, error) {

	log.Debugf("Looking for interface with '%v' addresses", addresses)

	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	nics := make(map[string]bool)
	for _, iface := range ifaces {
		addr, err := iface.Addrs()
		if err != nil {
			return nil, err
		}
		for _, address := range addr {
			log.Debugf("Checking '%s' address '%s'", iface.Name, address)
			for _, requestedAddr := range addresses {
				if requestedAddr == "0.0.0.0" {
					if (iface.Flags & net.FlagLoopback) == 0 {
						nics[iface.Name] = true
					}
				}
			}
		}
	}

	return utils.GetKeys(nics), nil
}

func GetFreePort() (uint, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return uint(l.Addr().(*net.TCPAddr).Port), nil
}
