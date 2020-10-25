package disk

import "syscall"

func getDevNumFromDeviceDirectory(stat syscall.Stat_t) uint64 {
	return uint64(stat.Rdev)
}

func getDevNumFromDirectory(stat syscall.Stat_t) uint64 {
	return uint64(stat.Dev)
}
