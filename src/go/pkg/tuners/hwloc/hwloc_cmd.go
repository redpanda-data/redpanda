package hwloc

import (
	"os/exec"
	"strconv"
	"strings"
	"vectorized/os"

	log "github.com/sirupsen/logrus"
)

type hwLocCmd struct {
	HwLoc
	proc os.Proc
}

func NewHwLocCmd(proc os.Proc) HwLoc {
	return &hwLocCmd{
		proc: proc,
	}
}

func (hwLocCmd *hwLocCmd) All() (string, error) {
	return hwLocCmd.runCalc("all")
}
func (hwLocCmd *hwLocCmd) Calc(mask string, location string) (string, error) {
	return hwLocCmd.runCalc(mask, location)
}

func (hwLocCmd *hwLocCmd) Distribute(numberOfElements uint) ([]string, error) {
	return hwLocCmd.runDistrib(strconv.Itoa(int(numberOfElements)))
}

func (hwLocCmd *hwLocCmd) DistributeRestrict(
	numberOfElements uint, mask string,
) ([]string, error) {
	return hwLocCmd.runDistrib(strconv.Itoa(int(numberOfElements)), "--single", "--restrict", mask)
}

func (hwLocCmd *hwLocCmd) GetNumberOfCores(mask string) (uint, error) {
	return hwLocCmd.getNumberOf(mask, "core")
}

func (hwLocCmd *hwLocCmd) GetNumberOfPUs(mask string) (uint, error) {
	return hwLocCmd.getNumberOf(mask, "PU")
}

func (hwLocCmd *hwLocCmd) getNumberOf(
	mask string, resource string,
) (uint, error) {
	output, err := hwLocCmd.runCalc("--number-of", resource, "machine:0", "--restrict", mask)
	count, err := strconv.Atoi(output)
	return uint(count), err
}

func (*hwLocCmd) CheckIfMaskIsEmpty(mask string) bool {
	for _, mask := range strings.Split(mask, ",") {
		i, _ := strconv.ParseInt(mask, 0, 32)
		if i != 0 {
			return false
		}
	}
	return true
}

func (*hwLocCmd) IsSupported() bool {
	log.Debug("Checking if 'hwloc' is present...")
	_, calcErr := exec.LookPath("hwloc-calc")
	_, distribErr := exec.LookPath("hwloc-distrib")
	if calcErr != nil || distribErr != nil {
		log.Info("Unable to find 'hwloc', install 'hwloc' package")
		return false
	}
	return true
}

func (hwLocCmd *hwLocCmd) runCalc(args ...string) (string, error) {
	outputLines, err := hwLocCmd.proc.Run("hwloc-calc", args...)
	if err != nil {
		return "", err
	}
	return outputLines[0], nil
}

func (hwLocCmd *hwLocCmd) runDistrib(args ...string) ([]string, error) {
	var result []string
	outputLines, err := hwLocCmd.proc.Run("hwloc-distrib", args...)
	if err != nil {
		return nil, err
	}
	for _, line := range outputLines {
		if line != "" {
			result = append(result, line)
		}
	}
	return result, nil
}
