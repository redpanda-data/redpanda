package os

import (
	"os"
	"syscall"
)

type fileLock struct {
	path  string
	flock *syscall.Flock_t
}

func lockFile(path string) (*fileLock, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	flock := &syscall.Flock_t{}
	err = syscall.FcntlFlock(f.Fd(), syscall.F_RDLCK, flock)
	if err != nil {
		return nil, err
	}
	return &fileLock{path, flock}, nil
}

func (f *fileLock) unlock() error {
	file, err := os.Open(f.path)
	if err != nil {
		return err
	}
	return syscall.FcntlFlock(file.Fd(), syscall.F_UNLCK, f.flock)
}

func CheckLocked(path string) (bool, error) {
	_, err := os.Stat(path)
	if err != nil {
		// If the file doesn't exist, we can just say it's not locked.
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	f, err := lockFile(path)
	if err != nil {
		return true, err
	}
	return false, f.unlock()
}
