/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "absl/container/btree_set.h"
#include "absl/container/flat_hash_map.h"
#include "base/format_to.h"
#include "cluster/errc.h"
#include "container/chunked_hash_map.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "serde/envelope.h"
#include "serde/rw/enum.h"
#include "serde/rw/envelope.h"
#include "serde/rw/map.h"
#include "serde/rw/optional.h"
#include "serde/rw/set.h"
#include "serde/rw/vector.h"

namespace cluster {

struct node_disk_space {
    model::node_id node_id;
    uint64_t total = 0;
    uint64_t used = 0;
    // total size of partitions moved to this node
    uint64_t assigned = 0;
    // total size of partitions moved from this node
    uint64_t released = 0;

    node_disk_space(model::node_id node_id, uint64_t total, uint64_t used);
    double original_used_ratio() const { return double(used) / total; }

    double peak_used_ratio() const { return double(used + assigned) / total; }

    double final_used_ratio() const;

    fmt::iterator format_to(fmt::iterator it) const;
};

struct partition_balancer_violations
  : serde::envelope<
      partition_balancer_violations,
      serde::version<0>,
      serde::compat_version<0>> {
    struct unavailable_node
      : serde::envelope<
          unavailable_node,
          serde::version<0>,
          serde::compat_version<0>> {
        model::node_id id;
        model::timestamp unavailable_since;

        unavailable_node() noexcept = default;
        unavailable_node(model::node_id id, model::timestamp unavailable_since);

        fmt::iterator format_to(fmt::iterator it) const;

        auto serde_fields() { return std::tie(id, unavailable_since); }

        friend bool
        operator==(const unavailable_node&, const unavailable_node&) = default;
    };

    struct full_node
      : serde::
          envelope<full_node, serde::version<0>, serde::compat_version<0>> {
        model::node_id id;
        uint32_t disk_used_percent;

        full_node() noexcept = default;
        full_node(model::node_id id, uint32_t disk_used_percent);

        fmt::iterator format_to(fmt::iterator it) const;

        auto serde_fields() { return std::tie(id, disk_used_percent); }

        friend bool operator==(const full_node&, const full_node&) = default;
    };

    std::vector<unavailable_node> unavailable_nodes;
    std::vector<full_node> full_nodes;

    partition_balancer_violations() noexcept = default;

    partition_balancer_violations(
      std::vector<unavailable_node> un, std::vector<full_node> fn);

    fmt::iterator format_to(fmt::iterator it) const;

    auto serde_fields() { return std::tie(unavailable_nodes, full_nodes); }

    friend bool operator==(
      const partition_balancer_violations&,
      const partition_balancer_violations&) = default;

    bool is_empty() const {
        return unavailable_nodes.empty() && full_nodes.empty();
    }
};

enum class partition_balancer_status {
    off,
    starting,
    ready,
    in_progress,
    stalled,
};

fmt::iterator format_to(partition_balancer_status status, fmt::iterator);

struct partition_balancer_overview_request
  : serde::envelope<
      partition_balancer_overview_request,
      serde::version<0>,
      serde::compat_version<0>> {
    fmt::iterator format_to(fmt::iterator it) const;
    auto serde_fields() { return std::tie(); }
};

/**
 * class describing a reason underlying partition replica set change
 */
enum class change_reason {
    rack_constraint_repair,
    partition_count_rebalancing,
    node_decommissioning,
    node_unavailable,
    disk_full,
    replica_pinning_repair,
};

fmt::iterator format_to(change_reason rep, fmt::iterator);
/**
 * Enum providing a details about partition replica reallocation failure.
 */
enum class reallocation_error : int8_t {
    missing_partition_size_info,
    no_eligible_node_found,
    over_partition_fd_limit,
    over_partition_memory_limit,
    over_partition_core_limit,
    no_quorum,
    reconfiguration_in_progress,
    partition_disabled,
    unknown_error,
};

fmt::iterator format_to(reallocation_error rep, fmt::iterator);

/**
 * Struct providing details about partition replica reallocation failure.
 * The details provided include the change reason, the replica that was
 * intended to be moved and the error.
 */
struct reallocation_failure_details
  : serde::envelope<
      reallocation_failure_details,
      serde::version<0>,
      serde::compat_version<0>> {
    model::node_id replica_to_move;
    change_reason reason;
    reallocation_error error;

    auto serde_fields() { return std::tie(replica_to_move, reason, error); }
    friend bool operator==(
      const reallocation_failure_details&,
      const reallocation_failure_details&) = default;

    fmt::iterator format_to(fmt::iterator it) const;
};

struct partition_balancer_overview_reply
  : serde::envelope<
      partition_balancer_overview_reply,
      serde::version<3>,
      serde::compat_version<0>> {
    partition_balancer_overview_reply() noexcept = default;
    partition_balancer_overview_reply(const partition_balancer_overview_reply&)
      = delete;
    partition_balancer_overview_reply(partition_balancer_overview_reply&&)
      = default;
    partition_balancer_overview_reply&
    operator=(const partition_balancer_overview_reply&) = delete;
    partition_balancer_overview_reply&
    operator=(partition_balancer_overview_reply&&) = default;

    errc error;
    model::timestamp last_tick_time;
    partition_balancer_status status;
    std::optional<partition_balancer_violations> violations;
    absl::flat_hash_map<model::node_id, absl::btree_set<model::ntp>>
      decommission_realloc_failures;
    size_t partitions_pending_force_recovery_count;
    std::vector<model::ntp> partitions_pending_force_recovery_sample;
    chunked_hash_map<model::ntp, reallocation_failure_details>
      reallocation_failures;

    void set_reallocation_failures(
      const chunked_hash_map<model::ntp, reallocation_failure_details>&
        reallocations);

    auto serde_fields() {
        return std::tie(
          error,
          last_tick_time,
          status,
          violations,
          decommission_realloc_failures,
          partitions_pending_force_recovery_count,
          partitions_pending_force_recovery_sample,
          reallocation_failures);
    }

    friend bool operator==(
      const partition_balancer_overview_reply&,
      const partition_balancer_overview_reply&) = default;

    fmt::iterator format_to(fmt::iterator it) const;

    partition_balancer_overview_reply copy() const;
};

class balancer_tick_aborted_exception final : public std::runtime_error {
public:
    explicit balancer_tick_aborted_exception(const std::string& msg)
      : std::runtime_error(msg) {}
};

} // namespace cluster
