package version

import "fmt"

var (
	version string
	rev     string
)

func Version() string {
	return version
}

func Rev() string {
	return rev
}

func Pretty() string {
	return fmt.Sprintf("%s (rev %s)", Version(), Rev())
}
