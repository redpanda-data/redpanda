package system

import "syscall"

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
