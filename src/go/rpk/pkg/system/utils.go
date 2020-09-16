package system

import (
	"syscall"
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

func uname() (string, error) {
	var uname syscall.Utsname
	err := syscall.Uname(&uname)
	if err != nil {
		return "", err
	}
	str := ""
	str += string(int8ToString(uname.Machine)) + " "
	str += string(int8ToString(uname.Release)) + " "
	str += string(int8ToString(uname.Version))
	return str, nil

}

func int8ToString(ints [65]int8) string {
	var bs [65]byte
	for i, in := range ints {
		bs[i] = byte(in)
	}
	return string(bs[:])
}
