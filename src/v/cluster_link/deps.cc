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

#include "cluster/members_table.h"
#include "cluster/security_frontend.h"
#include "cluster_link/utils.h"
#include "kafka/data/rpc/client.h"
#include "kafka/data/rpc/serde.h"
#include "security/authorizer.h"

namespace cluster_link {

namespace {
class security_impl : public security_service {
public:
    security_impl(
      ss::sharded<cluster::security_frontend>* security_fe,
      ss::sharded<security::authorizer>* authorizer)
      : _security_fe(security_fe)
      , _authorizer(authorizer) {}
    ss::future<chunked_vector<cluster::errc>> create_acls(
      chunked_vector<security::acl_binding> bindings,
      ::model::timeout_clock::duration timeout) final {
        return _security_fe->local().create_acls(std::move(bindings), timeout);
    }

    ss::future<chunked_hash_set<security::acl_binding>>
    describe_acls(chunked_vector<security::acl_binding_filter> filters) final {
        chunked_hash_set<security::acl_binding> bindings;
        for (const auto& filter : filters) {
            for (auto& binding : _authorizer->local().acls(filter)) {
                bindings.insert(std::move(binding));
            }
        }
        co_return bindings;
    }

    ss::future<chunked_vector<cluster::errc>> delete_acls(
      chunked_vector<security::acl_binding_filter> filters,
      ::model::timeout_clock::duration timeout) final {
        auto results = co_await _security_fe->local().delete_acls(
          std::move(filters), timeout);
        chunked_vector<cluster::errc> errcs;
        errcs.reserve(results.size());
        for (const auto& result : results) {
            errcs.emplace_back(result.error);
        }
        co_return errcs;
    }

private:
    ss::sharded<cluster::security_frontend>* _security_fe;
    ss::sharded<security::authorizer>* _authorizer;
};

class kafka_rpc_client_impl : public kafka_rpc_client_service {
public:
    explicit kafka_rpc_client_impl(
      ss::sharded<kafka::data::rpc::client>* client)
      : _client(client) {}

    ss::future<result<kafka::data::rpc::partition_offsets_map, cluster::errc>>
    get_partition_offsets(
      chunked_vector<kafka::data::rpc::topic_partitions> tps) final {
        return _client->local().get_partition_offsets(std::move(tps));
    }

private:
    ss::sharded<kafka::data::rpc::client>* _client;
};

class members_table_provider_impl : public members_table_provider {
public:
    explicit members_table_provider_impl(
      ss::sharded<cluster::members_table>* members_table)
      : _members_table(members_table) {}

    size_t node_count() const final {
        return _members_table->local().node_count();
    }

private:
    ss::sharded<cluster::members_table>* _members_table;
};
} // namespace

std::unique_ptr<security_service> security_service::make_default(
  ss::sharded<cluster::security_frontend>* security_fe,
  ss::sharded<security::authorizer>* authorizer) {
    return std::make_unique<security_impl>(security_fe, authorizer);
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

std::unique_ptr<members_table_provider> members_table_provider::make_default(
  ss::sharded<cluster::members_table>* members_table) {
    return std::make_unique<members_table_provider_impl>(members_table);
}
} // namespace cluster_link
