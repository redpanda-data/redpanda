package hwloc

import (
	"os/exec"
	"strconv"
	"strings"
	"time"
	"vectorized/pkg/os"

	log "github.com/sirupsen/logrus"
)

const (
	CalcBin    = "hwloc-calc-redpanda"
	DistribBin = "hwloc-distrib-redpanda"
)

type hwLocCmd struct {
	HwLoc
	proc    os.Proc
	timeout time.Duration
}

func NewHwLocCmd(proc os.Proc, timeout time.Duration) HwLoc {
	return &hwLocCmd{
		proc:    proc,
		timeout: timeout,
	}
}

func (hwLocCmd *hwLocCmd) All() (string, error) {
	return hwLocCmd.runCalc("all")
}

func (hwLocCmd *hwLocCmd) CalcSingle(mask string) (string, error) {
	return hwLocCmd.runCalc(mask)
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
	return hwLocCmd.runDistrib(strconv.Itoa(int(numberOfElements)),
		"--single", "--restrict", mask)
}

func (hwLocCmd *hwLocCmd) GetNumberOfCores(mask string) (uint, error) {
	return hwLocCmd.getNumberOf(mask, "core")
}

func (hwLocCmd *hwLocCmd) GetNumberOfPUs(mask string) (uint, error) {
	return hwLocCmd.getNumberOf(mask, "PU")
}

func (hwLocCmd *hwLocCmd) GetPhysIntersection(
	firstMask string, secondMask string,
) ([]uint, error) {
	out, err := hwLocCmd.runCalc("--intersect", firstMask, secondMask,
		"--physical")
	if err != nil {
		return nil, err
	}
	indices := []uint{}
	for _, idx := range strings.Split(out, ",") {
		intIdx, err := strconv.Atoi(strings.TrimSpace(idx))
		if err != nil {
			return nil, err
		}
		indices = append(indices, uint(intIdx))
	}
	return indices, nil
}

func (hwLocCmd *hwLocCmd) getNumberOf(
	mask string, resource string,
) (uint, error) {
	output, err := hwLocCmd.runCalc("--number-of", resource, "machine:0",
		"--restrict", mask)
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
	log.Debugf("Checking if '%s' & '%s' are present...", CalcBin, DistribBin)
	_, calcErr := exec.LookPath(CalcBin)
	_, distribErr := exec.LookPath(DistribBin)
	if calcErr != nil {
		log.Debugf("Unable to find '%s'", CalcBin)
		return false
	}
	if distribErr != nil {
		log.Debugf("Unable to find '%s'", DistribBin)
		return false
	}
	return true
}

func (hwLocCmd *hwLocCmd) runCalc(args ...string) (string, error) {
	outputLines, err := hwLocCmd.proc.RunWithSystemLdPath(hwLocCmd.timeout, CalcBin, args...)
	if err != nil {
		return "", err
	}
	return outputLines[0], nil
}

func (hwLocCmd *hwLocCmd) runDistrib(args ...string) ([]string, error) {
	var result []string
	outputLines, err := hwLocCmd.proc.RunWithSystemLdPath(hwLocCmd.timeout, DistribBin, args...)
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
