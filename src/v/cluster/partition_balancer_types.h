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

#include "cluster/errc.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timestamp.h"
#include "serde/envelope.h"
#include "serde/rw/enum.h"
#include "serde/rw/envelope.h"
#include "serde/rw/map.h"
#include "serde/rw/optional.h"
#include "serde/rw/rw.h"
#include "serde/rw/set.h"
#include "serde/rw/vector.h"
#include "utils/human.h"
#include "utils/to_string.h"

#include <absl/container/btree_set.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

namespace cluster {

struct node_disk_space {
    model::node_id node_id;
    uint64_t total = 0;
    uint64_t used = 0;
    // total size of partitions moved to this node
    uint64_t assigned = 0;
    // total size of partitions moved from this node
    uint64_t released = 0;

    inline node_disk_space(
      model::node_id node_id, uint64_t total, uint64_t used)
      : node_id(node_id)
      , total(total)
      , used(used) {}

    double original_used_ratio() const { return double(used) / total; }

    double peak_used_ratio() const { return double(used + assigned) / total; }

    double final_used_ratio() const {
        // it sometimes may happen  that the partition replica size on one node
        // is out of date with the total used size reported by a node space
        // manager. This may lead to an overflow of final used ratio.
        if (released >= used + assigned) {
            return 0.0;
        }

        return double(used + assigned - released) / total;
    }

    friend std::ostream& operator<<(std::ostream& o, const node_disk_space& d) {
        fmt::print(
          o,
          "{{total: {}, used: {}, assigned: {}, released: {}; "
          "used ratios: orig: {:.4}, peak: {:.4}, final: {:.4}}}",
          human::bytes(d.total),
          human::bytes(d.used),
          human::bytes(d.assigned),
          human::bytes(d.released),
          d.original_used_ratio(),
          d.peak_used_ratio(),
          d.final_used_ratio());
        return o;
    }
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
        unavailable_node(model::node_id id, model::timestamp unavailable_since)
          : id(id)
          , unavailable_since(unavailable_since) {}

        friend std::ostream&
        operator<<(std::ostream& o, const unavailable_node& u) {
            fmt::print(o, "{{ id: {} since: {} }}", u.id, u.unavailable_since);
            return o;
        }

        auto serde_fields() { return std::tie(id, unavailable_since); }

        bool operator==(const unavailable_node& other) const {
            return id == other.id
                   && unavailable_since == other.unavailable_since;
        }
    };

    struct full_node
      : serde::
          envelope<full_node, serde::version<0>, serde::compat_version<0>> {
        model::node_id id;
        uint32_t disk_used_percent;

        full_node() noexcept = default;
        full_node(model::node_id id, uint32_t disk_used_percent)
          : id(id)
          , disk_used_percent(disk_used_percent) {}

        friend std::ostream& operator<<(std::ostream& o, const full_node& f) {
            fmt::print(
              o,
              "{{ id: {} disk_used_percent: {} }}",
              f.id,
              f.disk_used_percent);
            return o;
        }

        auto serde_fields() { return std::tie(id, disk_used_percent); }

        bool operator==(const full_node& other) const {
            return id == other.id
                   && disk_used_percent == other.disk_used_percent;
        }
    };

    std::vector<unavailable_node> unavailable_nodes;
    std::vector<full_node> full_nodes;

    partition_balancer_violations() noexcept = default;

    partition_balancer_violations(
      std::vector<unavailable_node> un, std::vector<full_node> fn)
      : unavailable_nodes(std::move(un))
      , full_nodes(std::move(fn)) {}

    friend std::ostream&
    operator<<(std::ostream& o, const partition_balancer_violations& v) {
        fmt::print(
          o,
          "{{ unavailable_nodes: {} full_nodes: {} }}",
          v.unavailable_nodes,
          v.full_nodes);
        return o;
    }

    auto serde_fields() { return std::tie(unavailable_nodes, full_nodes); }

    bool operator==(const partition_balancer_violations& other) const {
        return unavailable_nodes == other.unavailable_nodes
               && full_nodes == other.full_nodes;
    }

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

inline std::ostream&
operator<<(std::ostream& os, partition_balancer_status status) {
    switch (status) {
    case partition_balancer_status::off:
        os << "off";
        break;
    case partition_balancer_status::starting:
        os << "starting";
        break;
    case partition_balancer_status::ready:
        os << "ready";
        break;
    case partition_balancer_status::in_progress:
        os << "in_progress";
        break;
    case partition_balancer_status::stalled:
        os << "stalled";
        break;
    }
    return os;
}

struct partition_balancer_overview_request
  : serde::envelope<
      partition_balancer_overview_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    friend std::ostream&
    operator<<(std::ostream& o, const partition_balancer_overview_request&) {
        fmt::print(o, "{{}}");
        return o;
    }

    auto serde_fields() { return std::tie(); }
};

struct partition_balancer_overview_reply
  : serde::envelope<
      partition_balancer_overview_reply,
      serde::version<2>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    errc error;
    model::timestamp last_tick_time;
    partition_balancer_status status;
    std::optional<partition_balancer_violations> violations;
    absl::flat_hash_map<model::node_id, absl::btree_set<model::ntp>>
      decommission_realloc_failures;
    size_t partitions_pending_force_recovery_count;
    std::vector<model::ntp> partitions_pending_force_recovery_sample;

    auto serde_fields() {
        return std::tie(
          error,
          last_tick_time,
          status,
          violations,
          decommission_realloc_failures,
          partitions_pending_force_recovery_count,
          partitions_pending_force_recovery_sample);
    }

    bool operator==(const partition_balancer_overview_reply& other) const {
        return error == other.error && last_tick_time == other.last_tick_time
               && status == other.status && violations == other.violations
               && decommission_realloc_failures
                    == other.decommission_realloc_failures
               && partitions_pending_force_recovery_count
                    == other.partitions_pending_force_recovery_count
               && partitions_pending_force_recovery_sample
                    == other.partitions_pending_force_recovery_sample;
    }

    friend std::ostream&
    operator<<(std::ostream& o, const partition_balancer_overview_reply& rep) {
        fmt::print(
          o,
          "{{ error: {} last_tick_time: {} status: {} violations: {}, "
          "partitions_pending_force_recovery: {}}}",
          rep.error,
          rep.last_tick_time,
          rep.status,
          rep.violations,
          rep.partitions_pending_force_recovery_count);
        return o;
    }
};

class balancer_tick_aborted_exception final : public std::runtime_error {
public:
    explicit balancer_tick_aborted_exception(const std::string& msg)
      : std::runtime_error(msg) {}
};

} // namespace cluster
