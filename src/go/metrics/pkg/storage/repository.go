package storage

import "time"

type Metrics struct {
	ReceivedAt    time.Time
	SentAt        time.Time `json:"sentAt,omitempty"`
	Organization  string    `json:"organization,omitempty"`
	ClusterId     string    `json:"clusterId,omitempty"`
	NodeId        int       `json:"nodeId,omitempty"`
	NodeUuid      string    `json:"nodeUuid,omitempty"`
	FreeMemory    uint64    `json:"freeMemory,omitempty"`
	FreeSpace     uint64    `json:"freeSpace,omitempty"`
	CpuPercentage uint64    `json:"cpuPercentage,omitempty"`
}

type Repository interface {
	SaveMetrics(m Metrics) error
}
