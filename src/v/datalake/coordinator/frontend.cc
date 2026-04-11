/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/coordinator/frontend.h"

#include "cluster/metadata_cache.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "cluster/topics_frontend.h"
#include "datalake/coordinator/coordinator_manager.h"
#include "datalake/coordinator/types.h"
#include "datalake/logger.h"
#include "hashing/murmur.h"
#include "raft/group_manager.h"
#include "rpc/connection_cache.h"

namespace datalake::coordinator {

namespace {
errc to_rpc_errc(coordinator::errc e) {
    switch (e) {
    case coordinator::errc::shutting_down:
    case coordinator::errc::not_leader:
        return errc::not_leader;
    case coordinator::errc::stm_apply_error:
        return errc::stale;
    case coordinator::errc::revision_mismatch:
        return errc::revision_mismatch;
    case coordinator::errc::incompatible_schema:
        return errc::incompatible_schema;
    case coordinator::errc::timedout:
        return errc::timeout;
    case coordinator::errc::failed:
        return errc::failed;
    }
}
ss::future<ensure_table_exists_reply> do_ensure_table_exists(
  coordinator_manager& mgr,
  model::ntp coordinator_ntp,
  ensure_table_exists_request req) {
    auto crd = mgr.get(coordinator_ntp);
    if (!crd) {
        co_return ensure_table_exists_reply{errc::not_leader};
    }
    auto ret = co_await crd->sync_ensure_table_exists(
      req.topic, req.topic_revision, std::move(req.schema_components));
    if (ret.has_error()) {
        co_return to_rpc_errc(ret.error());
    }
    co_return ensure_table_exists_reply{errc::ok};
}
ss::future<ensure_dlq_table_exists_reply> do_ensure_dlq_table_exists(
  coordinator_manager& mgr,
  model::ntp coordinator_ntp,
  ensure_dlq_table_exists_request req) {
    auto crd = mgr.get(coordinator_ntp);
    if (!crd) {
        co_return ensure_dlq_table_exists_reply{errc::not_leader};
    }
    auto ret = co_await crd->sync_ensure_dlq_table_exists(
      req.topic, req.topic_revision);
    if (ret.has_error()) {
        co_return to_rpc_errc(ret.error());
    }
    co_return ensure_dlq_table_exists_reply{errc::ok};
}
ss::future<add_translated_data_files_reply> add_files(
  coordinator_manager& mgr,
  model::ntp coordinator_ntp,
  add_translated_data_files_request req) {
    auto crd = mgr.get(coordinator_ntp);
    if (!crd) {
        co_return add_translated_data_files_reply{errc::not_leader};
    }
    auto ret = co_await crd->sync_add_files(
      req.tp, req.topic_revision, std::move(req.ranges));
    if (ret.has_error()) {
        co_return to_rpc_errc(ret.error());
    }
    co_return add_translated_data_files_reply{errc::ok};
}
ss::future<fetch_latest_translated_offset_reply> fetch_latest_offset(
  coordinator_manager& mgr,
  model::ntp coordinator_ntp,
  fetch_latest_translated_offset_request req) {
    auto crd = mgr.get(coordinator_ntp);
    if (!crd) {
        co_return fetch_latest_translated_offset_reply{errc::not_leader};
    }
    auto ret = co_await crd->sync_get_last_added_offsets(
      req.tp, req.topic_revision);
    if (ret.has_error()) {
        co_return to_rpc_errc(ret.error());
    }
    auto& val = ret.value();
    co_return fetch_latest_translated_offset_reply{
      val.last_added_offset, val.last_committed_offset};
}
} // namespace

template<auto Func, typename req_t>
requires requires(frontend::proto_t f, req_t req, ::rpc::client_opts opts) {
    (f.*Func)(std::move(req), std::move(opts));
}
auto frontend::remote_dispatch(req_t request, model::node_id leader_id) {
    using resp_t = req_t::resp_t;
    return _connection_cache->local()
      .with_node_client<proto_t>(
        _self,
        ss::this_shard_id(),
        leader_id,
        rpc_timeout,
        [request = std::move(request)](proto_t proto) mutable {
            return (proto.*Func)(
              std::move(request),
              ::rpc::client_opts{model::timeout_clock::now() + rpc_timeout});
        })
      .then(&::rpc::get_ctx_data<resp_t>)
      .then([leader_id, self = _self](result<resp_t> r) mutable {
          if (r.has_error()) {
              vlog(
                datalake::datalake_log.warn,
                "got error {} sending to coordinator leader {} from node {}",
                r.error().message(),
                leader_id,
                self);
              return resp_t{errc::timeout};
          }
          return std::move(r.value());
      });
}

template<auto LocalFunc, auto RemoteFunc, typename req_t>
requires requires(
  datalake::coordinator::frontend f, const model::ntp& ntp, req_t req) {
    (f.*LocalFunc)(std::move(req), ntp, ss::shard_id{0});
    request_has_topic<req_t> || request_has_coordinator_partition<req_t>;
}
auto frontend::process(req_t req, bool local_only) {
    using resp_t = req_t::resp_t;
    return ensure_topic_exists().then([req = std::move(req), local_only, this](
                                        bool exists) mutable {
        if (!exists) {
            return ss::make_ready_future<resp_t>(
              resp_t{errc::coordinator_topic_not_exists});
        }
        model::partition_id cp_result;
        if constexpr (request_has_topic<req_t>) {
            // If the request has a topic, we can use it to find the coordinator
            // partition.
            auto cp = coordinator_partition(req.get_topic());
            if (!cp) {
                return ss::make_ready_future<resp_t>(
                  resp_t{errc::coordinator_topic_not_exists});
            }
            cp_result = cp.value();
        } else {
            cp_result = req.get_coordinator_partition();
        }
        model::ntp c_ntp{
          model::datalake_coordinator_nt.ns,
          model::datalake_coordinator_nt.tp,
          cp_result};
        auto leader = _leaders->local().get_leader(c_ntp);
        if (leader == _self) {
            auto shard = _shard_table->local().shard_for(c_ntp);
            if (shard) {
                return (this->*LocalFunc)(
                  std::move(req), std::move(c_ntp), shard.value());
            }
        } else if (leader && !local_only) {
            return remote_dispatch<RemoteFunc>(std::move(req), leader.value());
        }
        return ss::make_ready_future<resp_t>(resp_t{errc::not_leader});
    });
}

// -- explicit instantiations ---
template auto
  frontend::remote_dispatch<&frontend::client::add_translated_data_files>(
    add_translated_data_files_request, model::node_id);

template auto frontend::process<
  &frontend::add_translated_data_files_locally,
  &frontend::client::add_translated_data_files>(
  add_translated_data_files_request, bool);

template auto
  frontend::remote_dispatch<&frontend::client::fetch_latest_translated_offset>(
    fetch_latest_translated_offset_request, model::node_id);

template auto frontend::process<
  &frontend::fetch_latest_translated_offset_locally,
  &frontend::client::fetch_latest_translated_offset>(
  fetch_latest_translated_offset_request, bool);

template auto frontend::remote_dispatch<&frontend::client::get_usage_stats>(
  usage_stats_request, model::node_id);

template auto frontend::process<
  &frontend::get_usage_stats_locally,
  &frontend::client::get_usage_stats>(usage_stats_request, bool);

template auto frontend::remote_dispatch<&frontend::client::get_topic_state>(
  get_topic_state_request, model::node_id);

template auto frontend::process<
  &frontend::get_topic_state_locally,
  &frontend::client::get_topic_state>(get_topic_state_request, bool);

template auto frontend::process<
  &frontend::reset_topic_state_locally,
  &frontend::client::reset_topic_state>(reset_topic_state_request, bool);

template auto frontend::remote_dispatch<&frontend::client::reset_topic_state>(
  reset_topic_state_request, model::node_id);

// -- explicit instantiations ---

frontend::frontend(
  model::node_id self,
  ss::sharded<coordinator_manager>* coordinator_mgr,
  ss::sharded<raft::group_manager>* group_mgr,
  ss::sharded<cluster::partition_manager>* partition_mgr,
  ss::sharded<cluster::topics_frontend>* topics_frontend,
  ss::sharded<cluster::metadata_cache>* metadata,
  ss::sharded<cluster::partition_leaders_table>* leaders,
  ss::sharded<cluster::shard_table>* shards,
  ss::sharded<::rpc::connection_cache>* connections)
  : _self(self)
  , _coordinator_mgr(coordinator_mgr)
  , _group_mgr(group_mgr)
  , _partition_mgr(partition_mgr)
  , _topics_frontend(topics_frontend)
  , _metadata(metadata)
  , _leaders(leaders)
  , _shard_table(shards)
  , _connection_cache(connections) {}

ss::future<> frontend::stop() { return _gate.close(); }

std::optional<model::partition_id>
frontend::coordinator_partition(const model::topic& topic) const {
    const auto md = _metadata->local().get_topic_metadata_ref(
      model::datalake_coordinator_nt);
    if (!md) {
        return std::nullopt;
    }
    auto bytes = iobuf_to_bytes(serde::to_iobuf(topic));
    auto partition = murmur2(bytes.data(), bytes.size())
                     % md->get().get_configuration().partition_count;
    return model::partition_id{static_cast<int32_t>(partition)};
}

std::optional<int32_t> frontend::coordinator_partition_count() const {
    const auto md = _metadata->local().get_topic_metadata_ref(
      model::datalake_coordinator_nt);
    if (!md) {
        return std::nullopt;
    }
    return md->get().get_configuration().partition_count;
}

ss::future<bool> frontend::ensure_topic_exists() {
    // todo: make these configurable.
    static constexpr int16_t default_replication_factor = 3;
    static constexpr int32_t default_coordinator_partitions = 3;

    const auto& metadata = _metadata->local();
    if (metadata.get_topic_metadata_ref(model::datalake_coordinator_nt)) {
        co_return true;
    }
    auto replication_factor = default_replication_factor;
    if (replication_factor > static_cast<int16_t>(metadata.node_count())) {
        replication_factor = 1;
    }

    cluster::topic_configuration topic{
      model::datalake_coordinator_nt.ns,
      model::datalake_coordinator_nt.tp,
      default_coordinator_partitions,
      replication_factor};

    topic.properties.compression = model::compression::none;
    // todo: fix this by implementing on demand raft
    // snapshots.
    topic.properties.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::none;
    topic.properties.retention_bytes = tristate<size_t>();
    topic.properties.retention_local_target_bytes = tristate<size_t>();
    topic.properties.retention_duration = tristate<std::chrono::milliseconds>();
    topic.properties.retention_local_target_ms
      = tristate<std::chrono::milliseconds>();

    try {
        auto res = co_await _topics_frontend->local().autocreate_topics(
          {std::move(topic)},
          config::shard_local_cfg().internal_rpc_request_timeout_ms());
        vassert(
          res.size() == 1,
          "Incorrect result when creating {}, expected 1 response, got: {}",
          model::datalake_coordinator_nt,
          res.size());
        if (
          res[0].ec != cluster::errc::success
          && res[0].ec != cluster::errc::topic_already_exists) {
            vlog(
              datalake::datalake_log.warn,
              "can not create topic: {} - error: {}",
              model::datalake_coordinator_nt,
              cluster::make_error_code(res[0].ec).message());
            co_return false;
        }
        co_return true;
    } catch (const std::exception_ptr& e) {
        vlog(
          datalake::datalake_log.warn,
          "can not create topic {} - exception: {}",
          model::datalake_coordinator_nt,
          e);
        co_return false;
    }
}

ss::future<ensure_table_exists_reply> frontend::ensure_table_exists_locally(
  ensure_table_exists_request request,
  const model::ntp& coordinator_partition,
  ss::shard_id shard) {
    co_return co_await _coordinator_mgr->invoke_on(
      shard,
      [coordinator_partition,
       req = std::move(request)](coordinator_manager& mgr) mutable {
          auto partition = mgr.get(coordinator_partition);
          if (!partition) {
              return ssx::now(ensure_table_exists_reply{errc::not_leader});
          }
          return do_ensure_table_exists(
            mgr, coordinator_partition, std::move(req));
      });
}

ss::future<ensure_table_exists_reply> frontend::ensure_table_exists(
  ensure_table_exists_request request, local_only local_only_exec) {
    auto holder = _gate.hold();
    co_return co_await process<
      &frontend::ensure_table_exists_locally,
      &client::ensure_table_exists>(std::move(request), bool(local_only_exec));
}

ss::future<ensure_dlq_table_exists_reply>
frontend::ensure_dlq_table_exists_locally(
  ensure_dlq_table_exists_request request,
  const model::ntp& coordinator_partition,
  ss::shard_id shard) {
    co_return co_await _coordinator_mgr->invoke_on(
      shard,
      [coordinator_partition,
       req = std::move(request)](coordinator_manager& mgr) mutable {
          auto partition = mgr.get(coordinator_partition);
          if (!partition) {
              return ssx::now(ensure_dlq_table_exists_reply{errc::not_leader});
          }
          return do_ensure_dlq_table_exists(
            mgr, coordinator_partition, std::move(req));
      });
}

ss::future<ensure_dlq_table_exists_reply> frontend::ensure_dlq_table_exists(
  ensure_dlq_table_exists_request request, local_only local_only_exec) {
    auto holder = _gate.hold();
    co_return co_await process<
      &frontend::ensure_dlq_table_exists_locally,
      &client::ensure_dlq_table_exists>(
      std::move(request), bool(local_only_exec));
}

ss::future<add_translated_data_files_reply>
frontend::add_translated_data_files_locally(
  add_translated_data_files_request request,
  const model::ntp& coordinator_partition,
  ss::shard_id shard) {
    co_return co_await _coordinator_mgr->invoke_on(
      shard,
      [coordinator_partition,
       req = std::move(request)](coordinator_manager& mgr) mutable {
          return add_files(mgr, coordinator_partition, std::move(req));
      });
}

ss::future<add_translated_data_files_reply> frontend::add_translated_data_files(
  add_translated_data_files_request request, local_only local_only_exec) {
    auto holder = _gate.hold();
    co_return co_await process<
      &frontend::add_translated_data_files_locally,
      &client::add_translated_data_files>(
      std::move(request), bool(local_only_exec));
}

ss::future<fetch_latest_translated_offset_reply>
frontend::fetch_latest_translated_offset_locally(
  fetch_latest_translated_offset_request request,
  const model::ntp& coordinator_partition,
  ss::shard_id shard) {
    co_return co_await _coordinator_mgr->invoke_on(
      shard,
      [coordinator_partition,
       req = std::move(request)](coordinator_manager& mgr) mutable {
          auto partition = mgr.get(coordinator_partition);
          if (!partition) {
              return ssx::now(
                fetch_latest_translated_offset_reply{errc::not_leader});
          }
          return fetch_latest_offset(
            mgr, coordinator_partition, std::move(req));
      });
}

ss::future<fetch_latest_translated_offset_reply>
frontend::fetch_latest_translated_offset(
  fetch_latest_translated_offset_request request, local_only local_only_exec) {
    auto holder = _gate.hold();
    co_return co_await process<
      &frontend::fetch_latest_translated_offset_locally,
      &client::fetch_latest_translated_offset>(
      std::move(request), bool(local_only_exec));
}

ss::future<usage_stats_reply> frontend::get_usage_stats_locally(
  usage_stats_request,
  const model::ntp& coordinator_partition,
  ss::shard_id shard) {
    auto holder = _gate.hold();
    co_return co_await _coordinator_mgr->invoke_on(
      shard, [coordinator_partition](coordinator_manager& mgr) mutable {
          auto partition = mgr.get(coordinator_partition);
          if (!partition) {
              return ssx::now(usage_stats_reply{errc::not_leader});
          }
          return partition->sync_get_usage_stats().then([](auto result) {
              usage_stats_reply resp;
              if (result.has_error()) {
                  resp.errc = to_rpc_errc(result.error());
              } else {
                  resp.errc = errc::ok;
                  resp.stats = std::move(result.value());
              }
              return ssx::now(std::move(resp));
          });
      });
}

ss::future<usage_stats_reply> frontend::get_usage_stats(
  usage_stats_request request, local_only local_only_exec) {
    auto holder = _gate.hold();
    co_return co_await process<
      &frontend::get_usage_stats_locally,
      &client::get_usage_stats>(std::move(request), bool(local_only_exec));
}

ss::future<get_topic_state_reply> frontend::get_topic_state_locally(
  get_topic_state_request request,
  const model::ntp& coordinator_partition,
  ss::shard_id shard) {
    auto holder = _gate.hold();
    co_return co_await _coordinator_mgr->invoke_on(
      shard,
      [coordinator_partition, &request](coordinator_manager& mgr) mutable {
          auto partition = mgr.get(coordinator_partition);
          if (!partition) {
              return ssx::now(get_topic_state_reply{errc::not_leader});
          }
          return partition
            ->sync_get_topic_state(std::move(request.topics_filter))
            .then([](auto result) {
                get_topic_state_reply resp{};
                if (result.has_error()) {
                    resp.errc = to_rpc_errc(result.error());
                } else {
                    resp.errc = errc::ok;
                    resp.topic_states = std::move(result.value());
                }
                return ssx::now(std::move(resp));
            });
      });
}

ss::future<get_topic_state_reply> frontend::get_topic_state(
  get_topic_state_request request, local_only local_only_exec) {
    auto holder = _gate.hold();
    co_return co_await process<
      &frontend::get_topic_state_locally,
      &client::get_topic_state>(std::move(request), bool(local_only_exec));
}

ss::future<get_topic_state_reply>
frontend::get_topic_state(chunked_vector<model::topic> topics_filter) {
    auto holder = _gate.hold();
    auto success = co_await ensure_topic_exists();
    if (!success) {
        co_return get_topic_state_reply{errc::coordinator_topic_not_exists};
    }
    chunked_hash_map<model::partition_id, chunked_vector<model::topic>>
      topics_by_partition;

    for (auto& topic : topics_filter) {
        auto partition_opt = coordinator_partition(topic);
        if (!partition_opt.has_value()) {
            co_return get_topic_state_reply{errc::coordinator_topic_not_exists};
        }
        topics_by_partition[partition_opt.value()].emplace_back(
          std::move(topic));
    }

    get_topic_state_reply aggregated;
    aggregated.errc = errc::ok;
    for (auto& [partition_id, filter] : topics_by_partition) {
        get_topic_state_request req{partition_id, std::move(filter)};
        auto reply = co_await get_topic_state(std::move(req));
        if (reply.errc != errc::ok) {
            co_return reply;
        }
        for (auto& [topic, state] : reply.topic_states) {
            aggregated.topic_states.insert({topic, std::move(state)});
        }
    }

    co_return aggregated;
}

ss::future<reset_topic_state_reply> frontend::reset_topic_state_locally(
  reset_topic_state_request request,
  const model::ntp& coordinator_partition,
  ss::shard_id shard) {
    auto holder = _gate.hold();
    co_return co_await _coordinator_mgr->invoke_on(
      shard,
      [coordinator_partition, &request](coordinator_manager& mgr) mutable {
          auto partition = mgr.get(coordinator_partition);
          if (!partition) {
              return ssx::now(reset_topic_state_reply{errc::not_leader});
          }
          return partition
            ->sync_reset_topic_state(
              request.topic,
              request.topic_revision,
              request.reset_all_partitions,
              std::move(request.partition_overrides))
            .then([](auto result) {
                reset_topic_state_reply resp{};
                if (result.has_error()) {
                    resp.errc = to_rpc_errc(result.error());
                } else {
                    resp.errc = errc::ok;
                }
                return ssx::now(std::move(resp));
            });
      });
}

ss::future<reset_topic_state_reply> frontend::reset_topic_state(
  reset_topic_state_request request, local_only local_only_exec) {
    auto holder = _gate.hold();
    co_return co_await process<
      &frontend::reset_topic_state_locally,
      &client::reset_topic_state>(std::move(request), bool(local_only_exec));
}

ss::future<checked<void, iceberg::catalog_describe_error>>
frontend::describe_catalog() {
    auto holder = _gate.hold();

    // This endpoint intentionally checks connectivity from the local node, so
    // it does not route through a coordinator leader.
    co_return co_await _coordinator_mgr->local().describe_catalog();
}

} // namespace datalake::coordinator
