// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

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
