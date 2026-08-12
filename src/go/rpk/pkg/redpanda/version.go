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
	vMatch := regexp.MustCompile(`^v?(\d+)\.(\d+)\.(\d+)(?:\s|-rc\d+|-dev|-nightly|$)`).FindStringSubmatch(s)

	if len(vMatch) == 0 {
		return Version{}, fmt.Errorf("unable to get the redpanda version from %q", s)
	}

	// The regexp guarantees each group is digits-only, but no longer bounds
	// how many: an implausibly long segment can still overflow int, so check
	// the conversion instead of ignoring its error.
	y, err := strconv.Atoi(vMatch[1])
	if err != nil {
		return Version{}, fmt.Errorf("unable to parse major version from %q: %v", s, err)
	}
	f, err := strconv.Atoi(vMatch[2])
	if err != nil {
		return Version{}, fmt.Errorf("unable to parse feature version from %q: %v", s, err)
	}
	p, err := strconv.Atoi(vMatch[3])
	if err != nil {
		return Version{}, fmt.Errorf("unable to parse patch version from %q: %v", s, err)
	}
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

// IsAtLeast returns true if the version is greater than or equal to the passed version.
func (v Version) IsAtLeast(b Version) bool {
	if v.Major != b.Major {
		return v.Major > b.Major
	}
	if v.Feature != b.Feature {
		return v.Feature > b.Feature
	}
	return v.Patch >= b.Patch
}

func (v Version) String() string {
	return fmt.Sprintf("%d.%d.%d", v.Major, v.Feature, v.Patch)
}
