package system

import "errors"

func uname() (string, error) {
	return "", errors.New("uname not available in MacOS")
}
