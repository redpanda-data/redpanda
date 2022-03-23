package featuregates

import "github.com/Masterminds/semver/v3"

const (
	centralizedConfigMajor = uint64(22)
	centralizedConfigMinor = uint64(1)
)

// CentralizedConfiguration feature gate should be removed when the operator
// will no longer support 21.x or older versions
func CentralizedConfiguration(version string) bool {
	v, err := semver.NewVersion(version)
	if err != nil {
		return false
	}

	return v.Major() == centralizedConfigMajor && v.Minor() >= centralizedConfigMinor || v.Major() > centralizedConfigMajor
}
