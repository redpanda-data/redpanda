// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package net

import (
	"net"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/utils"
	"go.uber.org/zap"
)

func GetInterfacesByIps(addresses ...string) ([]string, error) {
	zap.L().Sugar().Debugf("Looking for interface with '%v' addresses", addresses)

	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}

	for i, address := range addresses {
		resolvedIps, err := net.LookupIP(address)
		if err != nil {
			zap.L().Sugar().Debugf("Can't resolve address '%s', err: %s", address, err)
		}
		for _, resolvedIP := range resolvedIps {
			if resolvedIP.To4() != nil {
				addresses[i] = resolvedIP.String()
				break
			}
		}
	}

	nics := make(map[string]bool)
	for _, iface := range ifaces {
		if (iface.Flags & net.FlagLoopback) == net.FlagLoopback {
			continue
		}

		addr, err := iface.Addrs()
		if err != nil {
			return nil, err
		}
		for _, address := range addr {
			zap.L().Sugar().Debugf("Checking '%s' address '%s'", iface.Name, address)
			nicIP, _, err := net.ParseCIDR(address.String())
			if err != nil {
				zap.L().Sugar().Debugf("Skipping address '%s' as not a valid CIDR, err: %s", address, err)
				continue
			}
			for _, requestedAddr := range addresses {
				if requestedAddr == nicIP.String() || requestedAddr == "0.0.0.0" {
					nics[iface.Name] = true
				}
			}
		}
	}

	return utils.GetKeys(nics), nil
}

func getFreePort() (uint, error) {
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

func GetFreePortPool(n int) ([]uint, error) {
	m := make(map[uint]struct{})
	for len(m) != n {
		p, err := getFreePort()
		if err != nil {
			return nil, err
		}
		m[p] = struct{}{}
	}
	var ports []uint
	for port := range m {
		ports = append(ports, port)
	}
	return ports, nil
}
