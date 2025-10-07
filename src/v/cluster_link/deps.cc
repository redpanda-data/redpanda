/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster_link/deps.h"

#include "cluster/security_frontend.h"
#include "cluster_link/errc.h"
#include "cluster_link/utils.h"
#include "kafka/data/rpc/client.h"
#include "kafka/data/rpc/serde.h"

namespace cluster_link {

namespace {
class security_impl : public security_service {
public:
    explicit security_impl(ss::sharded<cluster::security_frontend>* security_fe)
      : _security_fe(security_fe) {}
    ss::future<std::vector<cluster::errc>> create_acls(
      std::vector<security::acl_binding> bindings,
      ::model::timeout_clock::duration timeout) final {
        return _security_fe->local().create_acls(std::move(bindings), timeout);
    }

private:
    ss::sharded<cluster::security_frontend>* _security_fe;
};

class kafka_rpc_client_impl : public kafka_rpc_client_service {
public:
    explicit kafka_rpc_client_impl(
      ss::sharded<kafka::data::rpc::client>* client)
      : _client(client) {}

    ss::future<
      result<kafka::data::rpc::delete_records_result_map, cluster::errc>>
    delete_records(kafka::data::rpc::delete_records_cmd_map cmds) final {
        return _client->local().delete_records(std::move(cmds));
    }

    ss::future<result<kafka::data::rpc::partition_offsets_map, cluster::errc>>
    get_partition_offsets(
      chunked_vector<kafka::data::rpc::topic_partitions> tps) final {
        return _client->local().get_partition_offsets(std::move(tps));
    }

private:
    ss::sharded<kafka::data::rpc::client>* _client;
};
} // namespace

std::unique_ptr<security_service> security_service::make_default(
  ss::sharded<cluster::security_frontend>* security_fe) {
    return std::make_unique<security_impl>(security_fe);
}

std::unique_ptr<kafka::client::cluster>
cluster_factory::create_cluster(const model::metadata& md) {
    return std::make_unique<kafka::client::cluster>(
      metadata_to_kafka_config(md));
}

std::unique_ptr<kafka_rpc_client_service>
kafka_rpc_client_service::make_default(
  ss::sharded<kafka::data::rpc::client>* client) {
    return std::make_unique<kafka_rpc_client_impl>(client);
}
} // namespace cluster_link
