package docker

type NodeContainerCfg struct {
	ClusterID   string
	ConfDir     string
	DataDir     string
	ContainerIP string
	NetworkName string
	RPCPort     int
	KafkaPort   int
	NodeID      int
}

type ContainerFactory interface {
	CreateNodeContainer(*NodeContainerCfg) error
}
