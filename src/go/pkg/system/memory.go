package system

import (
	"bufio"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/docker/go-units"
)

type MemInfo struct {
	MemTotal     int64
	MemFree      int64
	MemAvailable int64
}

func GetMemAvailableMB() (int, error) {
	mInfo, err := readMemInfo()
	if err != nil {
		return 0, err
	}
	return int(mInfo.MemAvailable / units.MiB), nil
}

func readMemInfo() (*MemInfo, error) {
	file, err := os.Open("/proc/meminfo")
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return parseMemInfo(file)
}

func parseMemInfo(reader io.Reader) (*MemInfo, error) {
	scanner := bufio.NewScanner(reader)
	var result = &MemInfo{}
	for scanner.Scan() {
		parts := strings.Fields(scanner.Text())
		if len(parts) < 3 || parts[2] != "kB" {
			continue
		}

		size, err := strconv.Atoi(parts[1])
		if err != nil {
			continue
		}
		bytes := int64(size) * units.KiB
		switch parts[0] {
		case "MemTotal:":
			result.MemTotal = bytes
		case "MemFree:":
			result.MemFree = bytes
		case "MemAvailable:":
			result.MemAvailable = bytes
		}
	}

	return result, nil
}
