package hwloc

type HwLoc interface {
	All() (string, error)
	CalcSingle(mask string) (string, error)
	Calc(mask string, location string) (string, error)
	Distribute(numberOfElements uint) ([]string, error)
	DistributeRestrict(numberOfElements uint, mask string) ([]string, error)
	GetNumberOfCores(mask string) (uint, error)
	GetNumberOfPUs(mask string) (uint, error)
	GetPhysIntersection(firstMask string, secondMask string) ([]uint, error)
	CheckIfMaskIsEmpty(mask string) bool
	IsSupported() bool
}
