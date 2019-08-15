package net

import (
	"net"
	"strings"
	"vectorized/pkg/utils"

	log "github.com/sirupsen/logrus"
)

func GetInterfacesByIp(ipAddress string) ([]string, error) {
	log.Debugf("Looking for interface with '%s' address", ipAddress)
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
			if ipAddress == "0.0.0.0" ||
				strings.HasPrefix(address.String(), ipAddress) {
				if (iface.Flags & net.FlagLoopback) == 0 {
					nics[iface.Name] = true
				}
			}
		}
	}

	return utils.GetKeys(nics), nil
}
