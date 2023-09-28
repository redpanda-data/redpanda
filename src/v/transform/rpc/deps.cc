/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "transform/rpc/deps.h"

#include "cluster/fwd.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "kafka/server/partition_proxy.h"
#include "model/ktp.h"

#include <seastar/core/do_with.hh>
#include <seastar/core/future.hh>

namespace transform::rpc {

namespace {
class partition_leader_cache_impl final : public partition_leader_cache {
public:
    explicit partition_leader_cache_impl(
      ss::sharded<cluster::partition_leaders_table>* table)
      : _table(table) {}

    std::optional<model::node_id> get_leader_node(
      model::topic_namespace_view tp_ns, model::partition_id p) const final {
        return _table->local().get_leader(tp_ns, p);
    }

private:
    ss::sharded<cluster::partition_leaders_table>* _table;
};
class topic_metadata_cache_impl final : public topic_metadata_cache {
public:
    explicit topic_metadata_cache_impl(
      ss::sharded<cluster::metadata_cache>* cache)
      : _cache(cache) {}

    std::optional<cluster::topic_configuration>
    find_topic_cfg(model::topic_namespace_view tp_ns) const final {
        return _cache->local().get_topic_cfg(tp_ns);
    }

    uint32_t get_default_batch_max_bytes() const final {
        return _cache->local().get_default_batch_max_bytes();
    };

private:
    ss::sharded<cluster::metadata_cache>* _cache;
};
class partition_manager_impl final : public partition_manager {
public:
    partition_manager_impl(
      ss::sharded<cluster::shard_table>* table,
      ss::sharded<cluster::partition_manager>* manager)
      : _table(table)
      , _manager(manager) {}

    std::optional<ss::shard_id> shard_owner(const model::ktp& ntp) final {
        return _table->local().shard_for(ntp);
    };
    std::optional<ss::shard_id> shard_owner(const model::ntp& ntp) final {
        return _table->local().shard_for(ntp);
    };

    ss::future<cluster::errc> invoke_on_shard(
      ss::shard_id shard,
      const model::ktp& ntp,
      ss::noncopyable_function<
        ss::future<cluster::errc>(kafka::partition_proxy*)> fn) final {
        return invoke_on_shard_impl(shard, ntp, std::move(fn));
    }
    ss::future<cluster::errc> invoke_on_shard(
      ss::shard_id shard,
      const model::ntp& ntp,
      ss::noncopyable_function<
        ss::future<cluster::errc>(kafka::partition_proxy*)> fn) final {
        return invoke_on_shard_impl(shard, ntp, std::move(fn));
    }

private:
    ss::future<cluster::errc> invoke_on_shard_impl(
      ss::shard_id shard,
      const model::any_ntp auto& ntp,
      ss::noncopyable_function<
        ss::future<cluster::errc>(kafka::partition_proxy*)> fn) {
        return _manager->invoke_on(
          shard,
          [ntp, fn = std::move(fn)](cluster::partition_manager& mgr) mutable {
              auto pp = kafka::make_partition_proxy(ntp, mgr);
              if (!pp || !pp->is_leader()) {
                  return ss::make_ready_future<cluster::errc>(
                    cluster::errc::not_leader);
              }
              return ss::do_with(
                *std::move(pp),
                [fn = std::move(fn)](kafka::partition_proxy& pp) {
                    return fn(&pp);
                });
          });
    }

    ss::sharded<cluster::shard_table>* _table;
    ss::sharded<cluster::partition_manager>* _manager;
};
} // namespace

std::unique_ptr<partition_leader_cache>
transform::rpc::partition_leader_cache::make_default(
  ss::sharded<cluster::partition_leaders_table>* table) {
    return std::make_unique<partition_leader_cache_impl>(table);
}

std::optional<model::node_id>
partition_leader_cache::get_leader_node(const model::ntp& ntp) const {
    return get_leader_node(model::topic_namespace_view(ntp), ntp.tp.partition);
}

std::unique_ptr<topic_metadata_cache>
transform::rpc::topic_metadata_cache::make_default(
  ss::sharded<cluster::metadata_cache>* cache) {
    return std::make_unique<topic_metadata_cache_impl>(cache);
}

std::unique_ptr<partition_manager>
transform::rpc::partition_manager::make_default(
  ss::sharded<cluster::shard_table>* table,
  ss::sharded<cluster::partition_manager>* manager) {
    return std::make_unique<partition_manager_impl>(table, manager);
}

std::optional<ss::shard_id>
partition_manager::shard_owner(const model::ktp& ktp) {
    return shard_owner(ktp.to_ntp());
}
ss::future<cluster::errc> partition_manager::invoke_on_shard(
  ss::shard_id shard_id,
  const model::ktp& ktp,
  ss::noncopyable_function<ss::future<cluster::errc>(kafka::partition_proxy*)>
    fn) {
    auto ntp = ktp.to_ntp();
    co_return co_await invoke_on_shard(shard_id, ntp, std::move(fn));
}
} // namespace transform::rpc
