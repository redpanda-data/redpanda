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

#include "cluster/controller.h"
#include "cluster/fwd.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "kafka/data/partition_proxy.h"
#include "logger.h"
#include "model/fundamental.h"
#include "model/ktp.h"
#include "model/transform.h"
#include "transform/stm/transform_offsets_stm.h"

#include <seastar/core/do_with.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/noncopyable_function.hh>

#include <memory>
#include <type_traits>

namespace transform::rpc {

namespace {

class partition_manager_impl final : public partition_manager {
public:
    partition_manager_impl(
      ss::sharded<cluster::shard_table>* table,
      ss::sharded<cluster::partition_manager>* manager,
      ss::smp_service_group smp_group)
      : _table(table)
      , _manager(manager)
      , _smp_group(smp_group) {}

    std::optional<ss::shard_id> shard_owner(const model::ktp& ntp) final {
        return _table->local().shard_for(ntp);
    };
    std::optional<ss::shard_id> shard_owner(const model::ntp& ntp) final {
        return _table->local().shard_for(ntp);
    };

    ss::future<result<model::offset, cluster::errc>> invoke_on_shard(
      ss::shard_id shard,
      const model::ktp& ktp,
      ss::noncopyable_function<ss::future<result<model::offset, cluster::errc>>(
        kafka::partition_proxy*)> fn) final {
        return invoke_on_shard_impl(shard, ktp, std::move(fn));
    }

    ss::future<result<model::wasm_binary_iobuf, cluster::errc>> invoke_on_shard(
      ss::shard_id shard,
      const model::ktp& ktp,
      ss::noncopyable_function<
        ss::future<result<model::wasm_binary_iobuf, cluster::errc>>(
          kafka::partition_proxy*)> fn) final {
        return invoke_on_shard_impl(shard, ktp, std::move(fn));
    }

    ss::future<result<model::offset, cluster::errc>> invoke_on_shard(
      ss::shard_id shard,
      const model::ntp& ntp,
      ss::noncopyable_function<ss::future<result<model::offset, cluster::errc>>(
        kafka::partition_proxy*)> fn) final {
        return invoke_on_shard_impl(shard, ntp, std::move(fn));
    }
    ss::future<result<model::wasm_binary_iobuf, cluster::errc>> invoke_on_shard(
      ss::shard_id shard,
      const model::ntp& ntp,
      ss::noncopyable_function<
        ss::future<result<model::wasm_binary_iobuf, cluster::errc>>(
          kafka::partition_proxy*)> fn) final {
        return invoke_on_shard_impl(shard, ntp, std::move(fn));
    }

    ss::future<find_coordinator_response> invoke_on_shard(
      ss::shard_id shard,
      const model::ntp& ntp,
      find_coordinator_request req) final {
        return invoke_func_on_shard_impl(
          shard,
          [this, ntp, req = std::move(req)](
            cluster::partition_manager& mgr) mutable {
              return do_find_coordinator(mgr.get(ntp), std::move(req));
          });
    }

    ss::future<offset_commit_response> invoke_on_shard(
      ss::shard_id shard,
      const model::ntp& ntp,
      offset_commit_request req) final {
        return invoke_func_on_shard_impl(
          shard,
          [this, ntp, req = std::move(req)](
            cluster::partition_manager& mgr) mutable {
              return do_offset_commit(mgr.get(ntp), std::move(req));
          });
    }

    ss::future<offset_fetch_response> invoke_on_shard(
      ss::shard_id shard,
      const model::ntp& ntp,
      offset_fetch_request req) final {
        return invoke_func_on_shard_impl(
          shard,
          [this, ntp, req = req](cluster::partition_manager& mgr) mutable {
              return do_offset_fetch(mgr.get(ntp), req);
          });
    }
    ss::future<result<model::transform_offsets_map, cluster::errc>>
    list_committed_offsets_on_shard(
      ss::shard_id shard, const model::ntp& ntp) final {
        return invoke_func_on_shard_impl(
          shard, [this, ntp](cluster::partition_manager& mgr) mutable {
              return do_list_committed_offsets(mgr.get(ntp));
          });
    }

    ss::future<cluster::errc> delete_committed_offsets_on_shard(
      ss::shard_id shard,
      const model::ntp& ntp,
      absl::btree_set<model::transform_id> ids) final {
        return invoke_func_on_shard_impl(
          shard,
          [this, ntp, ids = std::move(ids)](
            cluster::partition_manager& mgr) mutable {
              return do_delete_committed_offsets(mgr.get(ntp), std::move(ids));
          });
    }

private:
    static constexpr auto coordinator_partition = model::partition_id{0};

    ss::future<find_coordinator_response> do_find_coordinator(
      ss::lw_shared_ptr<cluster::partition> partition,
      find_coordinator_request request) {
        find_coordinator_response response;
        if (!partition) {
            for (const auto& key : request.keys) {
                response.errors[key] = cluster::errc::not_leader;
            }
            co_return response;
        }
        auto stm
          = partition->raft()->stm_manager()->get<transform_offsets_stm_t>();
        if (partition->ntp().tp.partition != coordinator_partition) {
            for (const auto& key : request.keys) {
                response.errors[key] = cluster::errc::not_leader;
            }
            co_return response;
        }
        for (const auto& key : request.keys) {
            auto result = co_await stm->coordinator(key);
            if (result.has_value()) {
                response.coordinators[key] = result.value();
            } else {
                response.errors[key] = result.error();
            }
        }
        co_return response;
    }

    ss::future<offset_commit_response> do_offset_commit(
      ss::lw_shared_ptr<cluster::partition> partition,
      offset_commit_request req) {
        offset_commit_response response{};
        if (req.kvs.empty()) {
            response.errc = cluster::errc::success;
            co_return response;
        }
        if (!partition) {
            response.errc = cluster::errc::not_leader;
            co_return response;
        }
        auto stm
          = partition->raft()->stm_manager()->get<transform_offsets_stm_t>();
        response.errc = co_await stm->put(std::move(req.kvs));
        co_return response;
    }

    ss::future<offset_fetch_response> do_offset_fetch(
      ss::lw_shared_ptr<cluster::partition> partition,
      offset_fetch_request request) {
        offset_fetch_response response{};
        if (!partition) {
            for (const auto& key : request.keys) {
                response.errors[key] = cluster::errc::not_leader;
            }
            co_return response;
        }
        auto stm
          = partition->raft()->stm_manager()->get<transform_offsets_stm_t>();
        for (const auto& key : request.keys) {
            auto result = co_await stm->get(key);
            if (result.has_error()) {
                response.errors[key] = result.error();
            } else {
                auto value = result.value();
                if (value.has_value()) {
                    response.results[key] = value.value();
                }
            }
        }
        co_return response;
    }

    ss::future<result<model::transform_offsets_map, cluster::errc>>
    do_list_committed_offsets(ss::lw_shared_ptr<cluster::partition> partition) {
        if (!partition) {
            co_return cluster::errc::not_leader;
        }
        auto stm
          = partition->raft()->stm_manager()->get<transform_offsets_stm_t>();
        co_return co_await stm->list();
    }

    ss::future<cluster::errc> do_delete_committed_offsets(
      ss::lw_shared_ptr<cluster::partition> partition,
      absl::btree_set<model::transform_id> ids) {
        if (!partition) {
            co_return cluster::errc::not_leader;
        }
        auto stm
          = partition->raft()->stm_manager()->get<transform_offsets_stm_t>();
        co_return co_await stm->remove_all(
          [&ids](model::transform_offsets_key key) {
              return ids.contains(key.id);
          });
    }

    template<typename R, typename NTP>
    ss::future<result<R, cluster::errc>> invoke_on_shard_impl(
      ss::shard_id shard,
      const NTP& ntp,
      ss::noncopyable_function<
        ss::future<result<R, cluster::errc>>(kafka::partition_proxy*)> func) {
        return invoke_func_on_shard_impl(
          shard,
          [ntp,
           func = std::move(func)](cluster::partition_manager& mgr) mutable {
              auto pp = kafka::make_partition_proxy(ntp, mgr);
              if (!pp || !pp->is_leader()) {
                  return ss::make_ready_future<result<R, cluster::errc>>(
                    cluster::errc::not_leader);
              }
              return ss::do_with(
                *std::move(pp),
                [func = std::move(func)](kafka::partition_proxy& pp) {
                    return func(&pp);
                });
          });
    }

    template<class Func>
    requires requires(Func f, cluster::partition_manager& mgr) { f(mgr); }
    std::invoke_result_t<Func, cluster::partition_manager&>
    invoke_func_on_shard_impl(ss::shard_id shard, Func&& func) {
        return _manager->invoke_on(
          shard, {_smp_group}, std::forward<Func>(func));
    }

    ss::sharded<cluster::shard_table>* _table;
    ss::sharded<cluster::partition_manager>* _manager;
    ss::smp_service_group _smp_group;
};

class cluster_members_cache_impl : public cluster_members_cache {
public:
    explicit cluster_members_cache_impl(
      ss::sharded<cluster::members_table>* table)
      : _table(table) {}

    std::vector<model::node_id> all_cluster_members() override {
        return _table->local().node_ids();
    }

private:
    ss::sharded<cluster::members_table>* _table;
};

} // namespace

std::unique_ptr<partition_manager>
transform::rpc::partition_manager::make_default(
  ss::sharded<cluster::shard_table>* table,
  ss::sharded<cluster::partition_manager>* manager,
  ss::smp_service_group smp_group) {
    return std::make_unique<partition_manager_impl>(table, manager, smp_group);
}

std::optional<ss::shard_id>
partition_manager::shard_owner(const model::ktp& ktp) {
    return shard_owner(ktp.to_ntp());
}

std::unique_ptr<cluster_members_cache>
cluster_members_cache::make_default(ss::sharded<cluster::members_table>* m) {
    return std::make_unique<cluster_members_cache_impl>(m);
}

} // namespace transform::rpc
