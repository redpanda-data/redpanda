package network

const (
	rfsTableSizeProperty = "net.core.rps_sock_flow_entries"
	listenBacklogFile    = "/proc/sys/net/core/somaxconn"
	synBacklogFile       = "/proc/sys/net/ipv4/tcp_max_syn_backlog"
	rfsTableSize         = 32768
	synBacklogSize       = 4096
	listenBacklogSize    = 4096
	maxInt               = int(^uint(0) >> 1)
)
