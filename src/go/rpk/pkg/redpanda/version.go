package redpanda

import (
	"fmt"
	"regexp"
	"strconv"
)

type Version struct {
	Major   int
	Feature int
	Patch   int
}

// VersionFromString creates a Version struct based on a passed string that
// contains the semver version string.
func VersionFromString(s string) (Version, error) {
	// Match the version of redpanda following semver convention, and returns:
	//   - index 0: the full match
	//   - index 1: the Major
	//   - index 2: the Feature
	//   - index 3: the Patch
	vMatch := regexp.MustCompile(`^v?(\d{1,2})\.(\d{1,2})\.(\d{1,2})(?:\s|-rc\d{1,2}|-dev|$)`).FindStringSubmatch(s)

	if len(vMatch) == 0 {
		return Version{}, fmt.Errorf("unable to get the redpanda version from %q", s)
	}

	// We can safely ignore the errors since we are making sure in the regexp
	// that we match digits only.
	y, _ := strconv.Atoi(vMatch[1])
	f, _ := strconv.Atoi(vMatch[2])
	p, _ := strconv.Atoi(vMatch[3])
	return Version{y, f, p}, nil
}

// Less returns true if the version is lower than the passed 'b' version.
func (v Version) Less(b Version) bool {
	if v.Major == b.Major {
		if v.Feature == b.Feature {
			return v.Patch < b.Patch
		}
		return v.Feature < b.Feature
	}
	return v.Major < b.Major
}

func (v Version) String() string {
	return fmt.Sprintf("%d.%d.%d", v.Major, v.Feature, v.Patch)
}
