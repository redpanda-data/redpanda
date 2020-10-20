package container

import (
	"net"
	"vectorized/pkg/config"
)

func applyPlatformSpecificConf(
	conf *config.Config, kafkaPort, rpcPort, seedRPCPort uint, ip, seedIP string,
) error {
	hostIP, err := getHostIP()
	if err != nil {
		return err
	}
	conf.Redpanda.KafkaApi.Address = ip
	conf.Redpanda.AdvertisedKafkaApi.Address = hostIP
	conf.Redpanda.AdvertisedKafkaApi.Port = int(kafkaPort)

	conf.Redpanda.RPCServer.Address = ip
	conf.Redpanda.AdvertisedRPCAPI.Address = hostIP
	conf.Redpanda.AdvertisedRPCAPI.Port = int(rpcPort)

	if seedIP != "" {
		conf.Redpanda.SeedServers = []*config.SeedServer{{
			Id: 0,
			Host: config.SocketAddress{
				Address: hostIP,
				Port:    int(seedRPCPort),
			},
		}}
	}
	return nil
}

func getHostIP() (string, error) {
	var cidr = "192.168.0.0/16"
	_, ipNet, err := net.ParseCIDR(cidr)
	if err != nil {
		return "", err
	}
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}

	for _, a := range addrs {
		ipnet, ok := a.(*net.IPNet)
		if !ok {
			continue
		}

		if ipNet.Contains(ipnet.IP) {
			return ipnet.IP.String(), nil
		}
	}
	return "", nil
}
