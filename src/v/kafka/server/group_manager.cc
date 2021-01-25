// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/group_manager.h"

#include "cluster/simple_batch_builder.h"
#include "kafka/protocol/delete_groups.h"
#include "kafka/protocol/describe_groups.h"
#include "kafka/protocol/offset_commit.h"
#include "kafka/protocol/offset_fetch.h"
#include "model/record.h"
#include "resource_mgmt/io_priority.h"

namespace kafka {

ss::future<> group_manager::start() {
    /*
     * receive notifications for partition leadership changes. when we become a
     * leader we recovery. when we become a follower (or the partition is
     * mapped to another node/core) the in-memory cache may be cleared.
     */
    _leader_notify_handle = _gm.local().register_leadership_notification(
      [this](
        raft::group_id group,
        [[maybe_unused]] model::term_id term,
        std::optional<model::node_id> leader_id) {
          auto p = _pm.local().partition_for(group);
          if (p) {
              handle_leader_change(p, leader_id);
          }
      });

    /*
     * receive notifications when group-metadata partitions come under
     * management on this core. note that the notify callback will be
     * synchronously invoked for all existing partitions that match the query.
     */
    _manage_notify_handle = _pm.local().register_manage_notification(
      model::kafka_internal_namespace,
      model::kafka_group_topic,
      [this](ss::lw_shared_ptr<cluster::partition> p) { attach_partition(p); });

    return ss::make_ready_future<>();
}

ss::future<> group_manager::stop() {
    _pm.local().unregister_manage_notification(_manage_notify_handle);
    _gm.local().unregister_leadership_notification(_leader_notify_handle);

    for (auto& e : _partitions) {
        e.second->as.request_abort();
    }

    return _gate.close();
}

void group_manager::attach_partition(ss::lw_shared_ptr<cluster::partition> p) {
    klog.debug("attaching group metadata partition {}", p->ntp());
    auto attached = ss::make_lw_shared<attached_partition>(p);
    auto res = _partitions.try_emplace(p->ntp(), attached);
    // TODO: this is not a forever assertion. this should just generally never
    // happen _now_ because we don't support partition migration / removal.
    // however, group manager is also not prepared for such scenarios.
    vassert(
      res.second, "double registration of ntp in group manager {}", p->ntp());
}

void group_manager::handle_leader_change(
  ss::lw_shared_ptr<cluster::partition> part,
  std::optional<model::node_id> leader) {
    (void)with_gate(_gate, [this, part, leader] {
        if (auto it = _partitions.find(part->ntp()); it != _partitions.end()) {
            return ss::with_semaphore(
              it->second->sem, 1, [this, p = it->second, leader] {
                  return handle_partition_leader_change(p, leader);
              });
        }
        return ss::make_ready_future<>();
    });
}

ss::future<> group_manager::inject_noop(
  ss::lw_shared_ptr<cluster::partition> p,
  [[maybe_unused]] ss::lowres_clock::time_point timeout) {
    cluster::simple_batch_builder builder(
      raft::data_batch_type, model::offset(0));
    group_log_record_key key{
      .record_type = group_log_record_key::type::noop,
    };
    builder.add_kv(std::move(key), iobuf());
    auto batch = std::move(builder).build();
    auto reader = model::make_memory_record_batch_reader(std::move(batch));

    // synchronization provided by raft after future resolves is sufficient to
    // get an up-to-date commit offset as an upperbound for our reader.
    return p
      ->replicate(
        std::move(reader),
        raft::replicate_options(raft::consistency_level::quorum_ack))
      .discard_result();
}

ss::future<> group_manager::handle_partition_leader_change(
  ss::lw_shared_ptr<attached_partition> p,
  std::optional<model::node_id> leader_id) {
    /*
     * TODO: when we are becoming a leader for this partition we'll recover
     * groups and commits from the log and re-populate the in-memory cache.
     * otherwise, we can remove any groups and commits that map to this
     * partition.
     */
    p->loading = true;
    if (leader_id == _self.id()) {
        auto timeout
          = ss::lowres_clock::now()
            + config::shard_local_cfg().kafka_group_recovery_timeout_ms();
        /*
         * we just became leader. make sure the log is up-to-date. see
         * struct group_log_record_key{} for more details.
         */
        return inject_noop(p->partition, timeout).then([this, timeout, p] {
            /*
             * the full log is read and deduplicated. the dedupe processing is
             * based on the record keys, so this code should be ready to
             * transparently take advantage of key-based compaction in the
             * future.
             */
            storage::log_reader_config reader_config(
              p->partition->start_offset(),
              model::model_limits<model::offset>::max(),
              0,
              std::numeric_limits<size_t>::max(),
              kafka_read_priority(),
              raft::data_batch_type,
              std::nullopt,
              std::nullopt);

            return p->partition->make_reader(reader_config)
              .then([this, p, timeout](model::record_batch_reader reader) {
                  return std::move(reader)
                    .consume(recovery_batch_consumer(&p->as), timeout)
                    .then([this, p](recovery_batch_consumer ctx) {
                        // avoid trying to recover if we stopped the reader
                        // because an abort was requested
                        if (p->as.abort_requested()) {
                            return ss::make_ready_future<>();
                        }
                        return recover_partition(p->partition, std::move(ctx))
                          .then([p] { p->loading = false; });
                    });
              });
        });
    } else {
        // TODO: we are not yet handling group / partition deletion
        return ss::make_ready_future<>();
    }
}

/*
 * TODO: this routine can be improved from a copy vs move perspective, but is
 * rather complicated at the moment to start having to also analyze all the data
 * dependencies that would support optimizing for moves.
 */
ss::future<> group_manager::recover_partition(
  ss::lw_shared_ptr<cluster::partition> p, recovery_batch_consumer ctx) {
    /*
     * [group-id -> [topic-partition -> offset-metadata]]
     */
    using offset_map_type = absl::flat_hash_map<
      kafka::group_id,
      absl::flat_hash_map<
        model::topic_partition,
        std::pair<model::offset, group_log_offset_metadata>>>;

    /*
     * build a mapping from group-id to (tp, offset) pairs where the latest
     * entries take precedence.
     */
    offset_map_type group_offsets;
    offset_map_type empty_group_offsets;

    for (auto& e : ctx.loaded_offsets) {
        model::topic_partition tp(e.first.topic, e.first.partition);
        if (ctx.loaded_groups.contains(e.first.group)) {
            group_offsets[e.first.group][tp] = e.second;
        } else {
            empty_group_offsets[e.first.group][tp] = e.second;
        }
    }

    for (auto& e : ctx.loaded_groups) {
        offset_map_type::mapped_type offsets; // default empty if not found
        if (auto it = group_offsets.find(e.first); it != group_offsets.end()) {
            offsets = it->second;
        }

        auto group = get_group(e.first);
        if (group) {
            klog.debug("group already exists {}", e.first);
            continue;
        }

        group = ss::make_lw_shared<kafka::group>(e.first, e.second, _conf, p);

        for (auto& e : offsets) {
            group->insert_offset(
              e.first,
              group::offset_metadata{
                e.second.first,
                e.second.second.offset,
                e.second.second.metadata.value_or(""),
              });
        }

        _groups.emplace(e.first, group);
        group->reschedule_all_member_heartbeats();
    }

    for (auto& e : empty_group_offsets) {
        auto group = get_group(e.first);
        if (group) {
            klog.debug("group already exists {}", e.first);
            continue;
        }

        group = ss::make_lw_shared<kafka::group>(
          e.first, group_state::empty, _conf, p);

        for (auto& e : e.second) {
            group->insert_offset(
              e.first,
              group::offset_metadata{
                e.second.first,
                e.second.second.offset,
                e.second.second.metadata.value_or(""),
              });
        }

        _groups.emplace(e.first, group);
        group->reschedule_all_member_heartbeats();
    }

    /*
     * <kafka>if the cache already contains a group which should be removed,
     * raise an error. Note that it is possible (however unlikely) for a
     * consumer group to be removed, and then to be used only for offset storage
     * (i.e. by "simple" consumers)</kafka>
     */
    for (auto& group_id : ctx.removed_groups) {
        if (
          _groups.contains(group_id)
          && !empty_group_offsets.contains(group_id)) {
            return ss::make_exception_future<>(
              std::runtime_error("unexpected unload of active group"));
        }
    }

    return ss::make_ready_future<>();
}

ss::future<ss::stop_iteration>
recovery_batch_consumer::operator()(model::record_batch batch) {
    if (unlikely(batch.header().type != raft::data_batch_type)) {
        klog.trace("ignorning batch with type {}", int(batch.header().type));
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::no);
    }
    if (as->abort_requested()) {
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::yes);
    }
    batch_base_offset = batch.base_offset();
    return ss::do_with(
             std::move(batch),
             [this](model::record_batch& batch) {
                 return model::for_each_record(batch, [this](model::record& r) {
                     return handle_record(std::move(r));
                 });
             })
      .then([] { return ss::stop_iteration::no; });
}

ss::future<> recovery_batch_consumer::handle_record(model::record r) {
    auto key = reflection::adl<group_log_record_key>{}.from(r.share_key());

    switch (key.record_type) {
    case group_log_record_key::type::group_metadata:
        return handle_group_metadata(std::move(key.key), r.release_value());

    case group_log_record_key::type::offset_commit:
        return handle_offset_metadata(std::move(key.key), r.release_value());

    case group_log_record_key::type::noop:
        // skip control structure
        return ss::make_ready_future<>();

    default:
        return ss::make_exception_future<>(std::runtime_error(fmt::format(
          "Unknown record type={} in group recovery", int(key.record_type))));
    }
}

ss::future<>
recovery_batch_consumer::handle_group_metadata(iobuf key_buf, iobuf val_buf) {
    auto group_id = kafka::group_id(
      reflection::from_iobuf<kafka::group_id::type>(std::move(key_buf)));

    auto metadata = reflection::from_iobuf<group_log_group_metadata>(
      std::move(val_buf));

    // TODO: this is primarily driven by _deleted_ partitions, and to a lesser
    // extent deleted groups. We'll soon handle the later case, but redpanda
    // doesn't consider yet the former.
    const bool tombstone = false;

    vlog(klog.trace, "Recovering group metadata {}", group_id);

    if (tombstone) {
        loaded_groups.erase(group_id);
        removed_groups.emplace(group_id);
    } else {
        removed_groups.erase(group_id);
        // until we switch over to a compacted topic or use raft snapshots,
        // always take the latest entry in the log.
        loaded_groups[group_id] = std::move(metadata);
    }

    return ss::make_ready_future<>();
}

ss::future<>
recovery_batch_consumer::handle_offset_metadata(iobuf key_buf, iobuf val_buf) {
    auto key = reflection::from_iobuf<group_log_offset_key>(std::move(key_buf));

    auto metadata = reflection::from_iobuf<group_log_offset_metadata>(
      std::move(val_buf));

    // TODO: handle for deleted groups and partitions. it isn't until kafka
    // v2.4.0 that delete offsets shows up as an actual api.
    const bool tombstone = false;

    vlog(klog.trace, "Recovering offset {} with metadata {}", key, metadata);

    if (tombstone) {
        loaded_offsets.erase(key);
    } else {
        // until we switch over to a compacted topic or use raft snapshots,
        // always take the latest entry in the log.
        loaded_offsets[key] = std::make_pair(
          batch_base_offset, std::move(metadata));
    }

    return ss::make_ready_future<>();
}

ss::future<join_group_response>
group_manager::join_group(join_group_request&& r) {
    klog.trace("join request {}", r);

    auto error = validate_group_status(
      r.ntp, r.data.group_id, join_group_api::key);
    if (error != error_code::none) {
        klog.trace("request validation failed with error={}", error);
        return make_join_error(r.data.member_id, error);
    }

    if (
      r.data.session_timeout_ms < _conf.group_min_session_timeout_ms()
      || r.data.session_timeout_ms > _conf.group_max_session_timeout_ms()) {
        klog.trace(
          "join group request has invalid session timeout min={}/{}/max={}",
          _conf.group_min_session_timeout_ms(),
          r.data.session_timeout_ms,
          _conf.group_max_session_timeout_ms());
        return make_join_error(
          r.data.member_id, error_code::invalid_session_timeout);
    }

    bool is_new_group = false;
    auto group = get_group(r.data.group_id);
    if (!group) {
        // <kafka>only try to create the group if the group is UNKNOWN AND
        // the member id is UNKNOWN, if member is specified but group does
        // not exist we should reject the request.</kafka>
        if (r.data.member_id != unknown_member_id) {
            klog.trace(
              "join request rejected for known member and unknown group");
            return make_join_error(
              r.data.member_id, error_code::unknown_member_id);
        }
        auto it = _partitions.find(r.ntp);
        if (it == _partitions.end()) {
            // the ntp's partition was available because we had to route the
            // request to the correct core, but when we looked again it was
            // gone. this is generally not going to be a scenario that can
            // happen until we have rebalancing / partition deletion feature.
            klog.error(
              "Partition not found for ntp {} joining group {}",
              r.ntp,
              r.data.group_id);
            return make_join_error(
              r.data.member_id, error_code::not_coordinator);
        }
        auto p = it->second->partition;
        group = ss::make_lw_shared<kafka::group>(
          r.data.group_id, group_state::empty, _conf, p);
        _groups.emplace(r.data.group_id, group);
        klog.trace("created new group {}", group);
        is_new_group = true;
    }

    return group->handle_join_group(std::move(r), is_new_group);
}

ss::future<sync_group_response>
group_manager::sync_group(sync_group_request&& r) {
    klog.trace("sync request {}", r);

    if (r.data.group_instance_id) {
        klog.trace("static group membership is unsupported");
        return make_sync_error(error_code::unsupported_version);
    }

    auto error = validate_group_status(
      r.ntp, r.data.group_id, sync_group_api::key);
    if (error != error_code::none) {
        klog.trace("invalid group status {}", error);
        if (error == error_code::coordinator_load_in_progress) {
            // <kafka>The coordinator is loading, which means we've lost the
            // state of the active rebalance and the group will need to start
            // over at JoinGroup. By returning rebalance in progress, the
            // consumer will attempt to rejoin without needing to rediscover the
            // coordinator. Note that we cannot return
            // COORDINATOR_LOAD_IN_PROGRESS since older clients do not expect
            // the error.</kafka>
            return make_sync_error(error_code::rebalance_in_progress);
        }
        return make_sync_error(error);
    }

    auto group = get_group(r.data.group_id);
    if (group) {
        return group->handle_sync_group(std::move(r));
    } else {
        klog.trace("group not found");
        return make_sync_error(error_code::unknown_member_id);
    }
}

ss::future<heartbeat_response> group_manager::heartbeat(heartbeat_request&& r) {
    klog.trace("heartbeat request {}", r);

    if (r.data.group_instance_id) {
        klog.trace("static group membership is unsupported");
        return make_heartbeat_error(error_code::unsupported_version);
    }

    auto error = validate_group_status(
      r.ntp, r.data.group_id, heartbeat_api::key);
    if (error != error_code::none) {
        klog.trace("invalid group status {}", error);
        if (error == error_code::coordinator_load_in_progress) {
            // <kafka>the group is still loading, so respond just
            // blindly</kafka>
            return make_heartbeat_error(error_code::none);
        }
        return make_heartbeat_error(error);
    }

    auto group = get_group(r.data.group_id);
    if (group) {
        return group->handle_heartbeat(std::move(r));
    }

    klog.trace("group not found");
    return make_heartbeat_error(error_code::unknown_member_id);
}

ss::future<leave_group_response>
group_manager::leave_group(leave_group_request&& r) {
    klog.trace("leave request {}", r);

    auto error = validate_group_status(
      r.ntp, r.data.group_id, leave_group_api::key);
    if (error != error_code::none) {
        klog.trace("invalid group status error={}", error);
        return make_leave_error(error);
    }

    auto group = get_group(r.data.group_id);
    if (group) {
        return group->handle_leave_group(std::move(r));
    } else {
        klog.trace("group does not exist");
        return make_leave_error(error_code::unknown_member_id);
    }
}

ss::future<offset_commit_response>
group_manager::offset_commit(offset_commit_request&& r) {
    auto error = validate_group_status(
      r.ntp, r.data.group_id, offset_commit_api::key);
    if (error != error_code::none) {
        return ss::make_ready_future<offset_commit_response>(
          offset_commit_response(r, error));
    }

    auto group = get_group(r.data.group_id);
    if (!group) {
        if (r.data.generation_id < 0) {
            // <kafka>the group is not relying on Kafka for group management, so
            // allow the commit</kafka>
            auto p = _partitions.find(r.ntp)->second->partition;
            group = ss::make_lw_shared<kafka::group>(
              r.data.group_id, group_state::empty, _conf, p);
            _groups.emplace(r.data.group_id, group);
        } else {
            // <kafka>or this is a request coming from an older generation.
            // either way, reject the commit</kafka>
            return ss::make_ready_future<offset_commit_response>(
              offset_commit_response(r, error_code::illegal_generation));
        }
    }

    return group->handle_offset_commit(std::move(r));
}

ss::future<offset_fetch_response>
group_manager::offset_fetch(offset_fetch_request&& r) {
    auto error = validate_group_status(
      r.ntp, r.data.group_id, offset_fetch_api::key);
    if (error != error_code::none) {
        return ss::make_ready_future<offset_fetch_response>(
          offset_fetch_response(error));
    }

    auto group = get_group(r.data.group_id);
    if (!group) {
        return ss::make_ready_future<offset_fetch_response>(
          offset_fetch_response(r.data.topics));
    }

    return group->handle_offset_fetch(std::move(r));
}

std::pair<bool, std::vector<listed_group>> group_manager::list_groups() const {
    auto loading = std::any_of(
      _partitions.cbegin(),
      _partitions.cend(),
      [](const std::
           pair<const model::ntp, ss::lw_shared_ptr<attached_partition>>& p) {
          return p.second->loading;
      });

    std::vector<listed_group> groups;
    for (const auto& it : _groups) {
        const auto& g = it.second;
        groups.push_back(
          {g->id(), g->protocol_type().value_or(protocol_type())});
    }

    return std::make_pair(loading, groups);
}

described_group
group_manager::describe_group(const model::ntp& ntp, const kafka::group_id& g) {
    auto error = validate_group_status(ntp, g, describe_groups_api::key);
    if (error != error_code::none) {
        return describe_groups_response::make_empty_described_group(g, error);
    }

    auto group = get_group(g);
    if (!group) {
        return describe_groups_response::make_dead_described_group(g);
    }

    return group->describe();
}

bool group_manager::valid_group_id(const group_id& group, api_key api) {
    switch (api) {
    case describe_groups_api::key:
    case offset_commit_api::key:
        [[fallthrough]];
    case offset_fetch_api::key:
        // <kafka> For backwards compatibility, we support the offset commit
        // APIs for the empty groupId, and also in DescribeGroups and
        // DeleteGroups so that users can view and delete state of all
        // groups.</kafka>
        return true;

    // currently unsupported apis
    case delete_groups_api::key:
        return false;

    // join-group etc... require non-empty group ids
    default:
        return !group().empty();
    }
}

/*
 * TODO
 * - check for group being shutdown
 */
error_code group_manager::validate_group_status(
  const model::ntp& ntp, const group_id& group, api_key api) {
    if (!valid_group_id(group, api)) {
        return error_code::invalid_group_id;
    }

    if (const auto it = _partitions.find(ntp); it != _partitions.end()) {
        if (!it->second->partition->is_leader()) {
            klog.trace("group partition is not leader {}/{}", group, ntp);
            return error_code::not_coordinator;
        }

        if (it->second->loading) {
            klog.trace("group is loading {}/{}", group, ntp);
            return error_code::not_coordinator;
            /*
             * returning `load in progress` is the correct error code for this
             * condition, and is what kafka brokers return. it should cause a
             * client to retry with backoff. however, it seems to be a rare
             * error condition in kafka (not in redpanda) and the sarama client
             * does not check for it (java and python both check properly).
             * sarama checks for `not coordinator` which does a metadata refresh
             * and a retry. it causes a bit more work in the client, but
             * achieves the same end result.
             *
             * See https://github.com/Shopify/sarama/issues/1715
             */
            // return error_code::coordinator_load_in_progress;
        }

        return error_code::none;
    }

    klog.trace("group operation misdirected {}/{}", group, ntp);
    return error_code::not_coordinator;
}

std::ostream& operator<<(std::ostream& os, const group_log_offset_key& key) {
    fmt::print(
      os,
      "group {} topic {} partition {}",
      key.group(),
      key.topic(),
      key.partition());
    return os;
}

std::ostream&
operator<<(std::ostream& os, const group_log_offset_metadata& md) {
    fmt::print(os, "offset {}", md.offset());
    return os;
}

} // namespace kafka
