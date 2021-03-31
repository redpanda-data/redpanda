package certmanager

import "fmt"

// cert-manager has limit of 64 bytes on the common name of certificate
const (
	nameLimit       = 64
	separatorLength = 1 // we use - as separator
)

// CommonName is certificate CN that is shortened to 64 chars
type CommonName string

// NewCommonName ensures the name does not exceed the limit of 64 bytes. It always
// shortens the cluster name and keeps the whole suffix.
// Suffix and name will be separated with -
func NewCommonName(clusterName, suffix string) CommonName {
	suffixLength := len(suffix)
	maxClusterNameLength := nameLimit - suffixLength - separatorLength
	if len(clusterName) > maxClusterNameLength {
		clusterName = clusterName[:maxClusterNameLength]
	}
	return CommonName(fmt.Sprintf("%s-%s", clusterName, suffix))
}
