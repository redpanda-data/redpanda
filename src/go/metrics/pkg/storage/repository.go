package storage

import "time"

type Metrics struct {
	ReceivedAt    time.Time
	SentAt        time.Time `json:"sentAt,omitempty"`
	Organization  string    `json:"organization,omitempty"`
	ClusterId     string    `json:"clusterId,omitempty"`
	NodeId        int       `json:"nodeId,omitempty"`
	NodeUuid      string    `json:"nodeUuid,omitempty"`
	FreeMemory    float64   `json:"freeMemory,omitempty"`
	FreeSpace     float64   `json:"freeSpace,omitempty"`
	CpuPercentage float64   `json:"cpuPercentage,omitempty"`
}

type Repository interface {
	SaveMetrics(m Metrics) error
}
