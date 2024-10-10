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
#include "datalake/coordinator/state_machine.h"
#include "datalake/coordinator/translated_offset_range.h"
#include "datalake/coordinator/types.h"
#include "datalake/logger.h"
#include "raft/group_manager.h"
#include "rpc/connection_cache.h"

namespace datalake::coordinator {

namespace {
coordinator_errc to_rpc_errc(coordinator::errc e) {
    switch (e) {
    case coordinator::errc::shutting_down:
    case coordinator::errc::not_leader:
        return coordinator_errc::not_leader;
    case coordinator::errc::stm_apply_error:
        return coordinator_errc::stale;
    case coordinator::errc::timedout:
        return coordinator_errc::timeout;
    }
}
ss::future<add_translated_data_files_reply> add_files(
  coordinator_manager& mgr,
  model::ntp coordinator_ntp,
  add_translated_data_files_request req) {
    auto crd = mgr.get(coordinator_ntp);
    if (!crd) {
        co_return add_translated_data_files_reply{coordinator_errc::not_leader};
    }
    chunked_vector<translated_offset_range> files;
    for (auto& fs : req.files) {
        // XXX: does each file need to be a vector?
        std::move(
          fs.translated_ranges.begin(),
          fs.translated_ranges.end(),
          std::back_inserter(files));
    }
    auto ret = co_await crd->sync_add_files(req.tp, std::move(files));
    if (ret.has_error()) {
        co_return to_rpc_errc(ret.error());
    }
    co_return add_translated_data_files_reply{coordinator_errc::ok};
}
ss::future<fetch_latest_data_file_reply> fetch_latest_offset(
  coordinator_manager& mgr,
  model::ntp coordinator_ntp,
  fetch_latest_data_file_request req) {
    auto crd = mgr.get(coordinator_ntp);
    if (!crd) {
        co_return fetch_latest_data_file_reply{coordinator_errc::not_leader};
    }
    auto ret = co_await crd->sync_get_last_added_offset(req.tp);
    if (ret.has_error()) {
        co_return to_rpc_errc(ret.error());
    }
    co_return fetch_latest_data_file_reply{ret.value()};
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
      .then([leader_id](result<resp_t> r) {
          if (r.has_error()) {
              vlog(
                datalake::datalake_log.warn,
                "got error {} on coordinator {}",
                r.error().message(),
                leader_id);
              return resp_t{datalake::coordinator_errc::timeout};
          }
          return r.value();
      });
}

template<auto LocalFunc, auto RemoteFunc, typename req_t>
requires requires(
  datalake::coordinator::frontend f, const model::ntp& ntp, req_t req) {
    (f.*LocalFunc)(std::move(req), ntp, ss::shard_id{0});
}
auto frontend::process(req_t req, bool local_only) {
    using resp_t = req_t::resp_t;
    return ensure_topic_exists().then([req = std::move(req), local_only, this](
                                        bool exists) mutable {
        if (!exists) {
            return ss::make_ready_future<resp_t>(
              resp_t{datalake::coordinator_errc::coordinator_topic_not_exists});
        }
        auto cp = coordinator_partition(req.topic_partition());
        model::ntp c_ntp{
          model::datalake_coordinator_nt.ns,
          model::datalake_coordinator_nt.tp,
          cp.value()};
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
        return ss::make_ready_future<resp_t>(
          resp_t{datalake::coordinator_errc::not_leader});
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
  frontend::remote_dispatch<&frontend::client::fetch_latest_data_file>(
    fetch_latest_data_file_request, model::node_id);

template auto frontend::process<
  &frontend::fetch_latest_data_file_locally,
  &frontend::client::fetch_latest_data_file>(
  fetch_latest_data_file_request, bool);

// -- explicit instantiations ---

frontend::frontend(
  model::node_id self,
  ss::sharded<coordinator_manager>* coordinator_mgr,
  ss::sharded<raft::group_manager>* group_mgr,
  ss::sharded<cluster::partition_manager>* partition_mgr,
  ss::sharded<cluster::topics_frontend>* topics_frontend,
  ss::sharded<cluster::metadata_cache>* metadata,
  ss::sharded<cluster::partition_leaders_table>* leaders,
  ss::sharded<cluster::shard_table>* shards)
  : _self(self)
  , _coordinator_mgr(coordinator_mgr)
  , _group_mgr(group_mgr)
  , _partition_mgr(partition_mgr)
  , _topics_frontend(topics_frontend)
  , _metadata(metadata)
  , _leaders(leaders)
  , _shard_table(shards) {}

ss::future<> frontend::stop() { return _gate.close(); }

std::optional<model::partition_id>
frontend::coordinator_partition(const model::topic_partition& tp) const {
    const auto md = _metadata->local().get_topic_metadata_ref(
      model::datalake_coordinator_nt);
    if (!md) {
        return std::nullopt;
    }
    iobuf temp;
    write(temp, tp);
    auto bytes = iobuf_to_bytes(temp);
    auto partition = murmur2(bytes.data(), bytes.size())
                     % md->get().get_configuration().partition_count;
    return model::partition_id{static_cast<int32_t>(partition)};
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
          config::shard_local_cfg().create_topic_timeout_ms());
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

ss::future<add_translated_data_files_reply>
frontend::add_translated_data_files_locally(
  add_translated_data_files_request request,
  const model::ntp& coordinator_partition,
  ss::shard_id shard) {
    co_return co_await _coordinator_mgr->invoke_on(
      shard,
      [&coordinator_partition,
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

ss::future<fetch_latest_data_file_reply>
frontend::fetch_latest_data_file_locally(
  fetch_latest_data_file_request request,
  const model::ntp& coordinator_partition,
  ss::shard_id shard) {
    co_return co_await _coordinator_mgr->invoke_on(
      shard,
      [&coordinator_partition,
       req = std::move(request)](coordinator_manager& mgr) mutable {
          auto partition = mgr.get(coordinator_partition);
          if (!partition) {
              return ssx::now(
                fetch_latest_data_file_reply{coordinator_errc::not_leader});
          }
          return fetch_latest_offset(
            mgr, coordinator_partition, std::move(req));
      });
}

ss::future<fetch_latest_data_file_reply> frontend::fetch_latest_data_file(
  fetch_latest_data_file_request request, local_only local_only_exec) {
    auto holder = _gate.hold();
    co_return co_await process<
      &frontend::fetch_latest_data_file_locally,
      &client::fetch_latest_data_file>(
      std::move(request), bool(local_only_exec));
}

} // namespace datalake::coordinator
