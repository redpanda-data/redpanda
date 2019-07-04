package labels

import "fmt"

const (
	NodeID    = "nodeId"
	ClusterID = "clusterId"
)

func NodeIDFilter(nodeID int) string {
	return fmt.Sprintf("%s=%d", NodeID, nodeID)
}

func ClusterIDFilter(clusterID string) string {
	return fmt.Sprintf("%s=%s", ClusterID, clusterID)
}
