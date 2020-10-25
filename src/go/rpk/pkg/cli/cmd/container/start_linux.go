package container

import "vectorized/pkg/config"

func applyPlatformSpecificConf(
	conf *config.Config, kafkaPort, rpcPort, _ uint, ip, seedIP string,
) error {
	conf.Redpanda.KafkaApi.Address = ip
	conf.Redpanda.AdvertisedKafkaApi.Address = ip

	conf.Redpanda.RPCServer.Address = ip
	conf.Redpanda.AdvertisedRPCAPI.Address = ip

	if seedIP != "" {
		conf.Redpanda.SeedServers = []*config.SeedServer{{
			Id: 0,
			Host: config.SocketAddress{
				Address: seedIP,
				Port:    int(config.DefaultConfig().Redpanda.RPCServer.Port),
			},
		}}
	}
	return nil
}
