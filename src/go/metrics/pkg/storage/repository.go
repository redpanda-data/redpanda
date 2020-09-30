package storage

import "time"

type Metrics struct {
	ReceivedAt    time.Time
	SentAt        time.Time `json:"sentAt,omitempty"`
	Organization  string    `json:"organization,omitempty"`
	ClusterId     string    `json:"clusterId,omitempty"`
	NodeId        int       `json:"nodeId,omitempty"`
	NodeUuid      string    `json:"nodeUuid,omitempty"`
	FreeMemoryMB  float64   `json:"freeMemoryMB,omitempty"`
	FreeSpaceMB   float64   `json:"freeSpaceMB,omitempty"`
	CpuPercentage float64   `json:"cpuPercentage,omitempty"`
}

type Environment struct {
	ReceivedAt   time.Time
	SentAt       time.Time `json:"sentAt,omitempty"`
	Organization string    `json:"organization,omitempty"`
	ClusterId    string    `json:"clusterId,omitempty"`
	NodeId       int       `json:"nodeId,omitempty"`
	NodeUuid     string    `json:"nodeUuid,omitempty"`
	Payload      string    `json:"payload"`
	Config       string    `json:"config"`
	CloudVendor  string    `json:"cloudVendor,omitempty"`
	VMType       string    `json:"vmType,omitempty"`
	OSInfo       string    `json:"osInfo,omitempty"`
	CPUModel     string    `json:"cpuModel,omitempty"`
	CPUCores     int       `json:"cpuCores,omitempty"`
	RPVersion    string    `json:"rpVersion,omitempty"`
	IP           string
	Country      string
	Region       string
	City         string
	Hostname     string
}

type Repository interface {
	SaveMetrics(m Metrics) error
	SaveEnvironment(e Environment) error
}
