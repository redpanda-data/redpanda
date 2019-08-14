package network

import (
	"github.com/safchain/ethtool"
)

type EthtoolWrapper interface {
	DriverName(string) (string, error)
	Features(string) (map[string]bool, error)
	Change(string, map[string]bool) error
}

func NewEthtoolWrapper() (EthtoolWrapper, error) {
	return ethtool.NewEthtool()
}
