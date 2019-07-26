package sandbox

import "github.com/docker/go-connections/nat"

type NodeState struct {
	Status        string
	Running       bool
	NodeDir       string
	HostRPCPort   int
	RPCPort       int
	KafkaPort     int
	HostKafkaPort int
	ID            int
	ContainerIP   string
	ContainerID   string
	PortMap       nat.PortMap
}
