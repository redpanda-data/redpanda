// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/cluster_utils.h"
#include "cluster/health_monitor_frontend.h"
#include "cluster/logger.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition.h"
#include "cluster/partition_manager.h"
#include "cluster/topic_table.h"
#include "config/configuration.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "kafka/protocol/delete_groups.h"
#include "kafka/protocol/describe_groups.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/offset_commit.h"
#include "kafka/protocol/offset_delete.h"
#include "kafka/protocol/offset_fetch.h"
#include "kafka/server/group.h"
#include "kafka/server/group_manager.h"
#include "kafka/server/group_metadata.h"
#include "kafka/server/group_tx_tracker_stm.h"
#include "kafka/server/logger.h"
#include "model/fundamental.h"
#include "ssx/async_algorithm.h"
#include "ssx/future-util.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>

#include <algorithm>
#include <chrono>
#include <limits>
#include <ranges>

using namespace std::chrono_literals;

namespace kafka {

ss::future<heartbeat_response> group_manager::heartbeat(heartbeat_request&& r) {
    auto error = validate_group_status(
      r.ntp, r.data.group_id, heartbeat_api::key, false);
    if (error != error_code::none) {
        if (error == error_code::coordinator_load_in_progress) {
            // <kafka>the group is still loading, so respond just
            // blindly</kafka>
            return make_heartbeat_error(error_code::none);
        }
        return make_heartbeat_error(error);
    }

    auto group = get_group(r.data.group_id);
    if (group) {
        return group->handle_heartbeat(std::move(r)).finally([group] {});
    }

    vlog(
      cg_klog.trace,
      "Cannot handle heartbeat request for unknown group {}",
      r.data.group_id);

    return make_heartbeat_error(error_code::unknown_member_id);
}

ss::future<leave_group_response>
group_manager::leave_group(leave_group_request&& r) {
    auto error = validate_group_status(
      r.ntp, r.data.group_id, leave_group_api::key, false);
    if (error != error_code::none) {
        return make_leave_error(error);
    }

    auto group = get_group(r.data.group_id);
    if (group) {
        return group->handle_leave_group(std::move(r)).finally([group] {});
    } else {
        vlog(
          cg_klog.trace,
          "Cannot handle leave group request for unknown group {}",
          r.data.group_id);
        if (r.version < api_version(3)) {
            return make_leave_error(error_code::unknown_member_id);
        }
        // since version 3 we need to fill each member error code
        leave_group_response response;
        response.data.members.reserve(r.data.members.size());
        std::transform(
          r.data.members.begin(),
          r.data.members.end(),
          std::back_inserter(response.data.members),
          [](member_identity& mid) {
              return member_response{
                .member_id = std::move(mid.member_id),
                .group_instance_id = std::move(mid.group_instance_id),
                .error_code = error_code::unknown_member_id,
              };
          });
        return ss::make_ready_future<leave_group_response>(std::move(response));
    }
}

ss::future<txn_offset_commit_response>
group_manager::txn_offset_commit(txn_offset_commit_request&& r) {
    auto p = get_attached_partition(r.ntp);
    if (!p || !p->partition->is_leader()) {
        return ss::make_ready_future<txn_offset_commit_response>(
          txn_offset_commit_response(r, error_code::not_coordinator));
    }
    auto maybe_holder = p->catchup_lock->try_hold_read_lock();
    if (!maybe_holder) {
        // transaction operations can't run in parallel with loading
        // state from the log (happens once per term change)
        vlog(
          cluster::txlog.trace,
          "can't process a tx: coordinator_load_in_progress");
        return ss::make_ready_future<txn_offset_commit_response>(
          txn_offset_commit_response(
            r, error_code::coordinator_load_in_progress));
    }
    // TODO: use correct key instead of offset_commit_api::key
    // check other txn places
    auto error = validate_group_status(
      r.ntp, r.data.group_id, offset_commit_api::key, false);
    if (error != error_code::none) {
        return ss::make_ready_future<txn_offset_commit_response>(
          txn_offset_commit_response(r, error));
    }

    auto group = get_group(r.data.group_id);
    if (!group) {
        // <kafka>the group is not relying on Kafka for group
        // management, so allow the commit</kafka>

        group = ss::make_lw_shared<kafka::group>(
          r.data.group_id,
          group_state::empty,
          _conf,
          p->catchup_lock,
          p->partition,
          p->term,
          _tx_frontend,
          _feature_table);
        _groups.emplace(r.data.group_id, group);
        _groups.rehash(0);
    }

    return group->handle_txn_offset_commit(std::move(r))
      .finally([unit = std::move(*maybe_holder), group] {});
}

ss::future<cluster::commit_group_tx_reply>
group_manager::commit_tx(cluster::commit_group_tx_request&& r) {
    auto p = get_attached_partition(r.ntp);
    if (!p || !p->partition->is_leader()) {
        return ss::make_ready_future<cluster::commit_group_tx_reply>(
          make_commit_tx_reply(cluster::tx::errc::not_coordinator));
    }
    auto maybe_holder = p->catchup_lock->try_hold_read_lock();
    if (!maybe_holder) {
        // transaction operations can't run in parallel with loading
        // state from the log (happens once per term change)
        vlog(
          cluster::txlog.trace,
          "can't process a tx: coordinator_load_in_progress");
        return ss::make_ready_future<cluster::commit_group_tx_reply>(
          make_commit_tx_reply(
            cluster::tx::errc::coordinator_load_in_progress));
    }
    auto error = validate_group_status(
      r.ntp, r.group_id, offset_commit_api::key, false);
    if (error != error_code::none) {
        if (error == error_code::not_coordinator) {
            return ss::make_ready_future<cluster::commit_group_tx_reply>(
              make_commit_tx_reply(cluster::tx::errc::not_coordinator));
        } else {
            return ss::make_ready_future<cluster::commit_group_tx_reply>(
              make_commit_tx_reply(cluster::tx::errc::timeout));
        }
    }

    auto group = get_group(r.group_id);
    if (!group) {
        return ss::make_ready_future<cluster::commit_group_tx_reply>(
          make_commit_tx_reply(cluster::tx::errc::timeout));
    }

    return group->handle_commit_tx(std::move(r))
      .finally([unit = std::move(*maybe_holder), group] {});
}

ss::future<cluster::begin_group_tx_reply>
group_manager::begin_tx(cluster::begin_group_tx_request&& r) {
    auto p = get_attached_partition(r.ntp);
    if (!p || !p->partition->is_leader()) {
        return ss::make_ready_future<cluster::begin_group_tx_reply>(
          make_begin_tx_reply(cluster::tx::errc::not_coordinator));
    }
    auto maybe_holder = p->catchup_lock->try_hold_read_lock();
    if (!maybe_holder) {
        // transaction operations can't run in parallel with loading
        // state from the log (happens once per term change)
        vlog(
          cluster::txlog.trace,
          "can't process a tx: coordinator_load_in_progress");
        return ss::make_ready_future<cluster::begin_group_tx_reply>(
          make_begin_tx_reply(cluster::tx::errc::coordinator_load_in_progress));
    }

    auto error = validate_group_status(
      r.ntp, r.group_id, offset_commit_api::key, false);
    if (error != error_code::none) {
        auto ec = error == error_code::not_coordinator
                    ? cluster::tx::errc::not_coordinator
                    : cluster::tx::errc::timeout;
        return ss::make_ready_future<cluster::begin_group_tx_reply>(
          make_begin_tx_reply(ec));
    }

    auto group = get_group(r.group_id);
    if (!group) {
        group = ss::make_lw_shared<kafka::group>(
          r.group_id,
          group_state::empty,
          _conf,
          p->catchup_lock,
          p->partition,
          p->term,
          _tx_frontend,
          _feature_table);
        _groups.emplace(r.group_id, group);
        _groups.rehash(0);
    }

    return group->handle_begin_tx(std::move(r))
      .finally([unit = std::move(*maybe_holder), group] {});
}

ss::future<cluster::abort_group_tx_reply>
group_manager::abort_tx(cluster::abort_group_tx_request&& r) {
    auto p = get_attached_partition(r.ntp);
    if (!p || !p->partition->is_leader()) {
        return ss::make_ready_future<cluster::abort_group_tx_reply>(
          make_abort_tx_reply(cluster::tx::errc::not_coordinator));
    }
    auto maybe_holder = p->catchup_lock->try_hold_read_lock();
    if (!maybe_holder) {
        // transaction operations can't run in parallel with loading
        // state from the log (happens once per term change)
        vlog(
          cluster::txlog.trace,
          "can't process a tx: coordinator_load_in_progress");
        return ss::make_ready_future<cluster::abort_group_tx_reply>(
          make_abort_tx_reply(cluster::tx::errc::coordinator_load_in_progress));
    }

    auto error = validate_group_status(
      r.ntp, r.group_id, offset_commit_api::key, true);
    if (error != error_code::none) {
        auto ec = error == error_code::not_coordinator
                    ? cluster::tx::errc::not_coordinator
                    : cluster::tx::errc::timeout;
        return ss::make_ready_future<cluster::abort_group_tx_reply>(
          make_abort_tx_reply(ec));
    }

    auto group = get_group(r.group_id);
    if (!group) {
        return ss::make_ready_future<cluster::abort_group_tx_reply>(
          make_abort_tx_reply(cluster::tx::errc::timeout));
    }

    return group->handle_abort_tx(std::move(r))
      .finally([unit = std::move(*maybe_holder), group] {});
}

ss::future<offset_delete_response>
group_manager::offset_delete(offset_delete_request&& r) {
    auto error = validate_group_status(
      r.ntp, r.data.group_id, offset_delete_api::key, false);
    if (error != error_code::none) {
        co_return offset_delete_response(error);
    }

    auto group = get_group(r.data.group_id);
    if (!group || group->in_state(group_state::dead)) {
        co_return offset_delete_response(error_code::group_id_not_found);
    }

    if (!group->in_state(group_state::empty) && !group->is_consumer_group()) {
        co_return offset_delete_response(error_code::non_empty_group);
    }

    chunked_vector<model::topic_partition> requested_deletions;
    for (const auto& topic : r.data.topics) {
        for (const auto& partition : topic.partitions) {
            requested_deletions.emplace_back(
              topic.name, partition.partition_index);
        }
    }

    auto deleted_offsets = group->delete_offsets(requested_deletions);
    co_await delete_offsets(group, deleted_offsets);

    chunked_hash_set<model::topic_partition> deleted_offsets_set;
    for (auto& tp : deleted_offsets) {
        deleted_offsets_set.insert(std::move(tp));
    }

    absl::flat_hash_map<
      model::topic,
      chunked_vector<offset_delete_response_partition>>
      response_data;
    for (const auto& tp : requested_deletions) {
        auto error = kafka::error_code::none;
        if (!deleted_offsets_set.contains(tp)) {
            error = kafka::error_code::group_subscribed_to_topic;
        }
        response_data[tp.topic].emplace_back(
          offset_delete_response_partition{
            .partition_index = tp.partition, .error_code = error});
    }

    offset_delete_response response(kafka::error_code::none);
    for (auto& [t, ps] : response_data) {
        response.data.topics.emplace_back(
          offset_delete_response_topic{.name = t, .partitions = std::move(ps)});
    }
    co_return response;
}

std::pair<error_code, chunked_vector<listed_group>>
group_manager::list_groups(const list_groups_filter_data& filter_data) const {
    auto loading = std::any_of(
      _partitions.cbegin(),
      _partitions.cend(),
      [](
        const std::
          pair<const model::ntp, ss::lw_shared_ptr<attached_partition>>& p) {
          return p.second->loading;
      });

    chunked_vector<listed_group> groups;
    for (const auto& it : _groups) {
        const auto& g = it.second;

        auto no_filter_specified = filter_data.states_filter.empty();
        auto matches_filter = filter_data.states_filter.contains(g->state());

        if (no_filter_specified || matches_filter) {
            groups.push_back(
              {g->id(),
               g->protocol_type().value_or(protocol_type()),
               group_state_to_kafka_name(g->state())});
        }
    }

    auto error = loading ? error_code::coordinator_load_in_progress
                         : error_code::none;

    return std::make_pair(error, std::move(groups));
}

described_group
group_manager::describe_group(const model::ntp& ntp, const kafka::group_id& g) {
    auto error = validate_group_status(ntp, g, describe_groups_api::key, true);
    if (error != error_code::none) {
        return describe_groups_response::make_empty_described_group(g, error);
    }

    auto group = get_group(g);
    if (!group) {
        return describe_groups_response::make_dead_described_group(g);
    }

    return group->describe();
}

group_manager::partition_producers
group_manager::describe_partition_producers(const model::ntp& ntp) {
    vlog(cg_klog.debug, "describe producers: {}", ntp);
    partition_producers response;
    response.partition_index = ntp.tp.partition;
    auto it = _partitions.find(ntp);
    if (it == _partitions.end() || !it->second->partition->is_leader()) {
        response.error_code = error_code::not_leader_for_partition;
        return response;
    }
    response.error_code = kafka::error_code::none;
    // snapshot the list of groups attached to this partition
    chunked_vector<std::pair<group_id, group_ptr>> groups;
    std::copy_if(
      _groups.begin(),
      _groups.end(),
      std::back_inserter(groups),
      [&ntp](const auto& g_pair) {
          const auto& [group_id, group] = g_pair;
          return group->partition()->ntp() == ntp;
      });
    for (auto& [gid, group] : groups) {
        if (group->in_state(group_state::dead)) {
            continue;
        }
        auto partition = group->partition();
        if (!partition) {
            // unlikely, conservative check
            continue;
        }
        for (const auto& [id, state] : group->producers()) {
            auto& tx = state.transaction;
            int64_t start_offset = -1;
            if (tx && tx->begin_offset >= model::offset{0}) {
                start_offset = partition->get_offset_translator_state()
                                 ->from_log_offset(tx->begin_offset);
            }
            int64_t last_timetamp = -1;
            if (tx) {
                auto time_since_last_update = model::timeout_clock::now()
                                              - tx->last_update;
                auto last_update_ts
                  = (model::timestamp_clock::now() - time_since_last_update);
                last_timetamp = last_update_ts.time_since_epoch() / 1ms;
            }
            response.active_producers.push_back({
              .producer_id = id,
              .producer_epoch = state.epoch,
              .last_sequence = tx ? tx->tx_seq : -1,
              .last_timestamp = last_timetamp,
              .coordinator_epoch = -1,
              .current_txn_start_offset = start_offset,
            });
        }
    }
    return response;
}

ss::future<std::error_code> group_manager::empty_and_delete_groups(
  const model::ntp& co_ntp,
  const chunked_vector<group_id>& groups,
  model::revision_id revision_id) {
    if (!_feature_table.local().is_active(
          features::feature::consumer_groups_migrations)) {
        vlog(
          cg_klog.warn,
          "delete request for {} failed - consumer groups migrations "
          "feature is not active",
          co_ntp);
        co_return cluster::errc::feature_disabled;
    }
    auto p = get_attached_partition(co_ntp);
    if (!p) {
        vlog(
          cg_klog.warn,
          "delete request for {} failed - attached partition not found",
          co_ntp);
        co_return cluster::errc::partition_not_exists;
    }
    auto maybe_holder = p->catchup_lock->try_hold_read_lock();
    if (!maybe_holder) {
        vlog(
          cluster::txlog.trace,
          "can't set blocked: coordinator_load_in_progress");
        co_return cluster::tx::errc::coordinator_load_in_progress;
    }
    auto block_lock_holder = co_await p->block_lock.get_units();

    // make sure all groups are blocked, and they have been blocked by a
    // revision id in the past
    for (const auto& group : groups) {
        auto it = p->group_blocks.find(group);
        if (it == p->group_blocks.end() || !it->second.is_blocked) {
            vlog(
              cg_klog.warn,
              "Group {} not blocked on ntp {}, cannot delete",
              group,
              co_ntp);
            co_return cluster::errc::invalid_configuration_update;
        }
        if (
          revision_id != model::revision_id{}
          && revision_id <= it->second.revision_id) {
            vlog(
              cg_klog.warn,
              "Group {} revision is {}, cannot delete with revision id {} on "
              "ntp {}",
              group,
              it->second.revision_id,
              revision_id,
              co_ntp);
            co_return cluster::errc::invalid_configuration_update;
        }
    }

    co_await ssx::async_for_each(
      groups, [this, &co_ntp](const group_id& group) {
          auto g = get_group(group);
          if (!g) {
              vlog(cg_klog.warn, "Group {} not found on ntp {}", group, co_ntp);
              return;
          }
          g->remove_full_members();
      });

    chunked_vector<std::pair<model::ntp, group_id>> groups_with_ntps{
      std::from_range,
      groups | std::views::transform([&co_ntp](const group_id& group_id) {
          return std::make_pair(co_ntp, group_id);
      })};
    auto delete_results = co_await delete_groups(
      std::move(groups_with_ntps), true);
    auto codes = std::views::transform(
      delete_results, &deletable_group_result::error_code);
    auto first_bad_code = std::ranges::find_if(codes, [](const auto& ec) {
        return ec != kafka::error_code::none
               && ec != kafka::error_code::group_id_not_found;
    });
    if (first_bad_code == codes.end()) {
        co_return std::error_code{};
    }
    co_return std::error_code{*first_bad_code};
}

ss::future<chunked_vector<deletable_group_result>> group_manager::delete_groups(
  chunked_vector<std::pair<model::ntp, group_id>> groups, bool allow_blocked) {
    chunked_vector<deletable_group_result> results;
    vlog(cg_klog.trace, "Deleting {} groups", groups.size());

    for (auto& group_info : groups) {
        auto error = validate_group_status(
          group_info.first,
          group_info.second,
          delete_groups_api::key,
          allow_blocked);
        if (error != error_code::none) {
            results.push_back(
              deletable_group_result{
                .group_id = std::move(group_info.second),
                .error_code = error_code::not_coordinator,
              });
            continue;
        }

        auto group = get_group(group_info.second);
        if (!group) {
            results.push_back(
              deletable_group_result{
                .group_id = std::move(group_info.second),
                .error_code = error_code::group_id_not_found,
              });
            continue;
        }

        // TODO: future optimizations
        // - handle group deletions in parallel
        // - batch tombstones same backing partition
        error = co_await group->remove();
        if (error == error_code::none) {
            group->pre_shutdown();
            _groups.erase(group_info.second);
        }
        results.push_back(
          deletable_group_result{
            .group_id = std::move(group_info.second),
            .error_code = error,
          });
    }

    _groups.rehash(0);

    co_return std::move(results);
}

ss::future<cluster::get_producers_reply>
group_manager::get_group_producers_locally(
  cluster::get_producers_request request) {
    const auto& ntp = request.ntp;
    cluster::get_producers_reply reply;
    auto it = _partitions.find(ntp);
    if (it == _partitions.end() || !it->second->partition->is_leader()) {
        reply.error_code = cluster::tx::errc::not_coordinator;
        co_return reply;
    }
    auto attached_partition = *it;
    reply.error_code = cluster::tx::errc::none;
    // snapshot the list of groups attached to this partition
    chunked_hash_map<group_id, group_ptr> groups;
    std::copy_if(
      _groups.begin(),
      _groups.end(),
      std::inserter(groups, groups.end()),
      [&ntp](auto g_pair) {
          const auto& [group_id, group] = g_pair;
          return group->partition()->ntp() == ntp;
      });
    reply.producer_count = std::accumulate(
      groups.begin(),
      groups.end(),
      size_t(0),
      [](size_t acc, const auto& entry) {
          return acc + entry.second->producers().size();
      });
    for (auto& [gid, group] : groups) {
        if (reply.producers.size() >= request.max_producers_to_include) {
            break;
        }
        if (group->in_state(group_state::dead)) {
            continue;
        }
        auto partition = group->partition();
        if (!partition) {
            // unlikely, conservative check
            continue;
        }
        for (const auto& [id, state] : group->producers()) {
            if (reply.producers.size() >= request.max_producers_to_include) {
                break;
            }
            cluster::producer_state_info producer_info;
            producer_info.pid = {id, state.epoch};
            producer_info.group_id = group->id()();
            auto& tx = state.transaction;
            if (tx) {
                producer_info.tx_begin_offset = tx->begin_offset;
                producer_info.tx_seq = tx->tx_seq;
                producer_info.tx_timeout = tx->timeout;
                auto time_since_last_update = model::timeout_clock::now()
                                              - tx->last_update;
                auto last_update_ts = model::timestamp_clock::now()
                                      - time_since_last_update;
                producer_info.last_update = model::timestamp{
                  last_update_ts.time_since_epoch() / 1ms};
                producer_info.coordinator_partition = tx->coordinator_partition;
            }
            reply.producers.push_back(std::move(producer_info));
        }
    }

    // check if there any any additional (stale) groups being tracked by
    // the stm, the list should be empty in most cases unless there is
    // a divergence in state.
    auto partition = attached_partition.second->partition;
    auto stm
      = partition->raft()->stm_manager()->get<kafka::group_tx_tracker_stm>();
    if (!stm) {
        co_return reply;
    }
    const auto& stm_txes = stm->inflight_transactions();
    for (const auto& [gid, state] : stm_txes) {
        if (groups.contains(gid)) {
            continue;
        }
        // we don't enforce size limits here because this list is expected to be
        // small. stale group found, report it to the dbug output.
        for (const auto& [pid, state] : state.producer_states) {
            reply.producers.push_back({
              .pid = pid,
              .tx_begin_offset = state.begin_offset,
              .group_id = gid() + "-stale",
            });
        }
    }
    co_return reply;
}

ss::future<> group_manager::collect_consumer_lag_metrics() {
    vlog(cg_klog.trace, "group_manager::collect_consumer_lag_metrics");
    vassert(
      ss::this_shard_id() == cluster::health_monitor_backend_shard,
      "collect_consumer_lag_metrics must run on shard {}",
      cluster::health_monitor_backend_shard);

    using lag = size_t;
    using topic_map_t = cluster::partitions_filter::topic_map_t;

    constexpr auto collect_ntps = [](const auto& gm) {
        topic_map_t topic_map;
        for (const auto& group : gm._groups | std::views::values) {
            for (const auto& tp : group->offsets() | std::views::keys) {
                topic_map[tp.topic].insert(tp.partition);
            }
        }
        return topic_map;
    };

    constexpr auto reduce_ntps = [](topic_map_t acc, topic_map_t val) {
        for (auto& [topic, parts] : val) {
            acc[topic].insert(parts.begin(), parts.end());
        }
        return acc;
    };

    auto ntps = co_await container().map_reduce0(
      collect_ntps, topic_map_t{}, reduce_ntps);

    if (ntps.empty()) {
        co_return;
    }

    auto report_r = co_await _hm_frontend.local().get_cluster_health(
      {.node_report_filter{.ntp_filters{
        .namespaces = {{model::kafka_namespace, std::move(ntps)}}}}},
      cluster::force_refresh::no,
      model::timeout_clock::now() + _lag_collection_interval());
    if (!report_r) {
        vlog(
          klog.warn,
          "group_manager::collect_consumer_lag_metrics: "
          "failed to get cluster health report: {}",
          report_r.error());
        co_return;
    }

    static constexpr auto find_partition_hwm =
      [](
        const cluster::cluster_health_report& response,
        const model::topic_partition& tp) -> std::optional<kafka::offset> {
        std::optional<kafka::offset> max_hwm;
        for (const auto& report : response.node_reports) {
            const model::topic_namespace_view tn{
              model::kafka_namespace, tp.topic};
            auto topic_it = report->topics.find(tn);
            if (topic_it == report->topics.end()) {
                continue;
            }
            auto partition_it = topic_it->second.find(tp.partition);
            if (partition_it == topic_it->second.end()) {
                continue;
            }
            auto hwm = partition_it->second.high_watermark;
            if (!max_hwm || hwm > *max_hwm) {
                max_hwm = hwm;
            }
        }
        return max_hwm;
    };

    const auto set_metrics = [&report_r](const group_manager& gm) {
        for (const auto& group : gm._groups | std::views::values) {
            consumer_lag_metrics lag_metrics{};
            for (const auto& [tp, group_topic_offsets] : group->offsets()) {
                if (auto hwm = find_partition_hwm(report_r.value(), tp); hwm) {
                    auto committed_offset = offset_cast(
                      group_topic_offsets->metadata.offset);
                    lag part_lag{static_cast<lag>(
                      std::max(*hwm - committed_offset, offset{0}))};
                    lag_metrics.sum += part_lag;
                    lag_metrics.max = std::max(lag_metrics.max, part_lag);
                }
            }
            group->set_lag_metrics(lag_metrics);
        }
    };

    co_await container().invoke_on_all(set_metrics);
}

group::offset_commit_stages
group_manager::offset_commit(offset_commit_request&& r) {
    auto error = validate_group_status(
      r.ntp, r.data.group_id, offset_commit_api::key, false);
    if (error != error_code::none) {
        return group::offset_commit_stages(offset_commit_response(r, error));
    }

    auto group = get_group(r.data.group_id);
    if (!group) {
        if (r.data.generation_id < 0) {
            // <kafka>the group is not relying on Kafka for group management, so
            // allow the commit</kafka>
            auto p = _partitions.find(r.ntp)->second;
            group = ss::make_lw_shared<kafka::group>(
              r.data.group_id,
              group_state::empty,
              _conf,
              p->catchup_lock,
              p->partition,
              p->term,
              _tx_frontend,
              _feature_table);
            _groups.emplace(r.data.group_id, group);
            _groups.rehash(0);
        } else {
            // <kafka>or this is a request coming from an older generation.
            // either way, reject the commit</kafka>
            return group::offset_commit_stages(
              offset_commit_response(r, error_code::illegal_generation));
        }
    }

    auto stages = group->handle_offset_commit(std::move(r));
    stages.result = stages.result.finally([group] {});
    return stages;
}

ss::future<offset_fetch_response>
group_manager::offset_fetch(offset_fetch_request r) {
    const auto require_stable = r.data.require_stable;
    offset_fetch_response response;

    for (auto& g_req : r.data.groups) {
        auto& g_res = response.data.groups.emplace_back();
        auto error = validate_group_status(
          r.ntp, g_req.group_id, offset_fetch_api::key, true);
        if (error != error_code::none) {
            g_res.group_id = std::move(g_req.group_id);
            g_res.error_code = error;
            continue;
        }

        auto group = get_group(g_req.group_id);
        if (!group) {
            g_res = offset_fetch_response::make_group(std::move(g_req));
        } else {
            g_res = co_await group->handle_offset_fetch(
              std::move(g_req), require_stable);
        }
    }
    co_return response;
}

bool group_manager::valid_group_id(const group_id& group, api_key api) {
    switch (api) {
    case describe_groups_api::key:
    case offset_commit_api::key:
    case delete_groups_api::key:
        [[fallthrough]];
    case offset_fetch_api::key:
        // <kafka> For backwards compatibility, we support the offset commit
        // APIs for the empty groupId, and also in DescribeGroups and
        // DeleteGroups so that users can view and delete state of all
        // groups.</kafka>
        return true;

    // join-group etc... require non-empty group ids
    default:
        return !group().empty();
    }
}

error_code group_manager::validate_group_status(
  const model::ntp& ntp,
  const group_id& group,
  api_key api,
  bool allow_blocked) const {
    if (!valid_group_id(group, api)) {
        vlog(
          cg_klog.debug,
          "Group name {} is invalid for operation {}",
          group,
          api);
        return error_code::invalid_group_id;
    }

    if (const auto it = _partitions.find(ntp); it != _partitions.end()) {
        auto& p = it->second;
        if (!allow_blocked) {
            auto it = p->group_blocks.find(group);
            if (it != p->group_blocks.end() && it->second.is_blocked)
              [[unlikely]] {
                vlog(
                  cg_klog.debug,
                  "Group name {} is blocked, cannot perform operation {}",
                  group,
                  api);
                return error_code::invalid_group_id;
            }
        }

        if (!p->partition->is_leader()) {
            vlog(
              cg_klog.debug,
              "Group {} operation {} sent to non-leader coordinator {}",
              group,
              api,
              ntp);
            return error_code::not_coordinator;
        }
        if (p->partition->term() != p->term()) {
            vlog(
              cg_klog.info,
              "Group {} operation {} for partition {} processed while term "
              "changed. "
              "Group term: {} current term: {}",
              group,
              api,
              ntp,
              p->term(),
              p->partition->term());
            return error_code::not_coordinator;
        }

        if (p->loading) {
            vlog(
              cg_klog.debug,
              "Group {} operation {} sent to loading coordinator {}",
              group,
              api,
              ntp);
            return error_code::not_coordinator;
        }

        return error_code::none;
    }

    vlog(
      cg_klog.debug,
      "Group {} operation {} misdirected to non-coordinator {}",
      group,
      api,
      ntp);
    return error_code::not_coordinator;
}

} // namespace kafka
