package tuners

import "errors"

func UtsnameStr() string {
	return ""
}

func GetKernelVersion() (string, error) {
	return "", errors.New("Kernel version info not available in MacOS")
}
