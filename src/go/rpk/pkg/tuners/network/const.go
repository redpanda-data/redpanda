package network

const (
	RfsTableSizeProperty = "net.core.rps_sock_flow_entries"
	ListenBacklogFile    = "/proc/sys/net/core/somaxconn"
	SynBacklogFile       = "/proc/sys/net/ipv4/tcp_max_syn_backlog"
	RfsTableSize         = 32768
	SynBacklogSize       = 4096
	ListenBacklogSize    = 4096
	MaxInt               = int(^uint(0) >> 1)
)
