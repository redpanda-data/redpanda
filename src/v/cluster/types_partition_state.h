/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/errc.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "serde/rw/envelope.h"
#include "serde/rw/sstring.h"
#include "serde/rw/vector.h"

#include <seastar/core/sstring.hh>

#include <chrono>
#include <cstdint>
#include <optional>
#include <vector>

namespace cluster {

struct producer_id_lookup_request
  : serde::envelope<
      producer_id_lookup_request,
      serde::version<0>,
      serde::compat_version<0>> {
    producer_id_lookup_request() noexcept = default;
    auto serde_fields() { return std::tie(); }
};

struct producer_id_lookup_reply
  : serde::envelope<
      producer_id_lookup_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    cluster::errc ec{};
    model::producer_id highest_producer_id{};

    producer_id_lookup_reply() noexcept = default;
    explicit producer_id_lookup_reply(cluster::errc e)
      : ec(e) {}
    explicit producer_id_lookup_reply(model::producer_id pid)
      : highest_producer_id(pid) {}

    auto serde_fields() { return std::tie(ec, highest_producer_id); }
};

struct partition_state_request
  : serde::envelope<
      partition_state_request,
      serde::version<0>,
      serde::compat_version<0>> {
    model::ntp ntp;
    friend bool
    operator==(const partition_state_request&, const partition_state_request&)
      = default;

    auto serde_fields() { return std::tie(ntp); }
};

struct partition_stm_state
  : serde::envelope<
      partition_stm_state,
      serde::version<1>,
      serde::compat_version<0>> {
    ss::sstring name;
    model::offset last_applied_offset;
    model::offset max_removable_local_log_offset;
    model::offset last_local_snapshot_offset;

    auto serde_fields() {
        return std::tie(
          name,
          last_applied_offset,
          max_removable_local_log_offset,
          last_local_snapshot_offset);
    }
};

struct partition_raft_state
  : serde::envelope<
      partition_raft_state,
      serde::version<5>,
      serde::compat_version<0>> {
    model::node_id node;
    model::term_id term;
    ss::sstring offset_translator_state;
    ss::sstring group_configuration;
    model::term_id confirmed_term;
    model::offset flushed_offset;
    model::offset commit_index;
    model::offset majority_replicated_index;
    model::offset visibility_upper_bound_index;
    model::offset last_quorum_replicated_index;
    model::term_id last_snapshot_term;
    model::offset last_snapshot_index;
    model::offset received_snapshot_index;
    size_t received_snapshot_bytes;
    bool has_pending_flushes;
    bool is_leader;
    bool is_elected_leader;
    std::vector<partition_stm_state> stms;
    bool write_caching_enabled;
    size_t flush_bytes;
    std::chrono::milliseconds flush_ms;
    ss::sstring replication_monitor_state;
    std::chrono::milliseconds time_since_last_flush;

    struct follower_state
      : serde::envelope<
          follower_state,
          serde::version<0>,
          serde::compat_version<0>> {
        model::node_id node;
        model::offset last_flushed_log_index;
        model::offset last_dirty_log_index;
        model::offset match_index;
        model::offset next_index;
        model::offset expected_log_end_offset;
        size_t heartbeats_failed;
        bool is_learner;
        uint64_t ms_since_last_heartbeat;
        uint64_t last_sent_seq;
        uint64_t last_received_seq;
        uint64_t last_successful_received_seq;
        bool suppress_heartbeats;
        bool is_recovering;

        auto serde_fields() {
            return std::tie(
              node,
              last_flushed_log_index,
              last_dirty_log_index,
              match_index,
              next_index,
              expected_log_end_offset,
              heartbeats_failed,
              is_learner,
              ms_since_last_heartbeat,
              last_sent_seq,
              last_received_seq,
              last_successful_received_seq,
              suppress_heartbeats,
              is_recovering);
        }

        friend bool operator==(const follower_state&, const follower_state&)
          = default;
    };

    struct follower_recovery_state
      : serde::envelope<
          follower_recovery_state,
          serde::version<0>,
          serde::compat_version<0>> {
        bool is_active = false;
        int64_t pending_offset_count = 0;

        auto serde_fields() {
            return std::tie(is_active, pending_offset_count);
        }

        friend bool operator==(
          const follower_recovery_state&, const follower_recovery_state&)
          = default;
    };

    // Set only on leaders.
    std::optional<std::vector<follower_state>> followers;
    // Set only on recovering followers.
    std::optional<follower_recovery_state> recovery_state;

    auto serde_fields() {
        return std::tie(
          node,
          term,
          offset_translator_state,
          group_configuration,
          confirmed_term,
          flushed_offset,
          commit_index,
          majority_replicated_index,
          visibility_upper_bound_index,
          last_quorum_replicated_index,
          last_snapshot_term,
          last_snapshot_index,
          received_snapshot_index,
          received_snapshot_bytes,
          has_pending_flushes,
          is_leader,
          is_elected_leader,
          followers,
          stms,
          recovery_state,
          write_caching_enabled,
          flush_bytes,
          flush_ms,
          replication_monitor_state,
          time_since_last_flush);
    }

    friend bool
    operator==(const partition_raft_state&, const partition_raft_state&)
      = default;
};

struct partition_state
  : serde::
      envelope<partition_state, serde::version<3>, serde::compat_version<0>> {
    model::offset start_offset;
    model::offset committed_offset;
    model::offset last_stable_offset;
    model::offset high_water_mark;
    model::offset dirty_offset;
    model::offset latest_configuration_offset;
    model::offset start_cloud_offset;
    model::offset next_cloud_offset;
    model::revision_id revision_id;
    size_t log_size_bytes;
    size_t non_log_disk_size_bytes;
    bool is_read_replica_mode_enabled;
    bool is_remote_fetch_enabled;
    bool is_cloud_data_available;
    ss::sstring read_replica_bucket;
    ss::sstring iceberg_mode;
    partition_raft_state raft_state;
    model::offset max_tombstone_removable_offset;
    model::offset max_transaction_removable_offset;
    model::offset max_cleanly_compacted_offset;
    model::offset max_transaction_free_offset;

    auto serde_fields() {
        return std::tie(
          start_offset,
          committed_offset,
          last_stable_offset,
          high_water_mark,
          dirty_offset,
          latest_configuration_offset,
          start_cloud_offset,
          next_cloud_offset,
          revision_id,
          log_size_bytes,
          non_log_disk_size_bytes,
          is_read_replica_mode_enabled,
          is_remote_fetch_enabled,
          is_cloud_data_available,
          read_replica_bucket,
          raft_state,
          iceberg_mode,
          max_tombstone_removable_offset,
          max_transaction_removable_offset,
          max_cleanly_compacted_offset,
          max_transaction_free_offset);
    }

    friend bool operator==(const partition_state&, const partition_state&)
      = default;
};

struct partition_state_reply
  : serde::envelope<
      partition_state_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    model::ntp ntp;
    std::optional<partition_state> state;
    errc error_code;

    friend bool
    operator==(const partition_state_reply&, const partition_state_reply&)
      = default;

    auto serde_fields() { return std::tie(ntp, state, error_code); }
};

} // namespace cluster
