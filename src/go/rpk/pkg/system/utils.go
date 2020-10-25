package system

import (
	"time"
	"vectorized/pkg/os"

	log "github.com/sirupsen/logrus"
)

func UnameAndDistro(timeout time.Duration) (string, error) {
	res, err := uname()
	if err != nil {
		return "", err
	}
	cmd := "lsb_release"
	p := os.NewProc()
	ls, err := p.RunWithSystemLdPath(timeout, cmd, "-d", "-s")
	if err != nil {
		log.Debugf("%s failed", cmd)
	}
	if len(ls) == 0 {
		log.Debugf("%s didn't return any output", cmd)
	} else {
		res += " " + ls[0]
	}
	return res, nil
}

func int8ToString(ints [65]int8) string {
	var bs [65]byte
	for i, in := range ints {
		bs[i] = byte(in)
	}
	return string(bs[:])
}
