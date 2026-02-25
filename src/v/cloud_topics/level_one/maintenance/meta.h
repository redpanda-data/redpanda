/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/format_to.h"
#include "cloud_topics/level_one/metastore/metastore.h"
#include "cloud_topics/level_one/metastore/offset_interval_set.h"
#include "container/chunked_hash_map.h"
#include "container/intrusive_list_helpers.h"
#include "model/fundamental.h"
#include "model/timestamp.h"

#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

#include <memory>

namespace cloud_topics::l1 {

// Contains compaction information collected from the metastore and the time at
// which it was obtained.
struct compaction_info_and_timestamp {
    metastore::compaction_info_response info;
    model::timestamp collected_at;
    kafka::offset max_compactible_offset;
};

struct log_maintenance_meta {
    log_maintenance_meta(model::topic_id_partition tidp, model::ntp ntp)
      : tidp(std::move(tidp))
      , ntp(std::move(ntp)) {}

    model::topic_id_partition tidp;
    model::ntp ntp;
    // Whether this log is:
    // 1. `idle` (not yet queued for compaction)
    // 2. `queued` (present in the scheduler's `log_maintenance_queue`)
    // 3. `inflight` (currently undergoing a compaction on a worker shard)
    enum class log_state { idle, queued, inflight } state{log_state::idle};
    // If set, this is cached compaction metadata obtained from the metastore at
    // the `collected_at` time. Guaranteed to have a value if `state == queued`
    // or `state == inflight`.
    std::optional<compaction_info_and_timestamp> info_and_ts{std::nullopt};
    // If set, this is the shard on which the log is currently undergoing an
    // inflight compaction. Guaranteed to have a value if `state == inflight`.
    std::optional<ss::shard_id> inflight_shard{std::nullopt};
    intrusive_list_hook link;
    // If `true`, we have been able to sample compaction info from the
    // `metastore` previously.
    bool has_seen_reconciled_data{false};
};

using log_maintenance_meta_ptr = ss::lw_shared_ptr<log_maintenance_meta>;
using foreign_log_maintenance_meta_ptr
  = ss::foreign_ptr<log_maintenance_meta_ptr>;

struct log_maintenance_meta_hash {
    using is_transparent = void;

    size_t
    operator()(const cloud_topics::l1::log_maintenance_meta_ptr& m) const {
        return absl::Hash<model::topic_id_partition>{}(m->tidp);
    }

    size_t operator()(const model::topic_id_partition& tidp) const {
        return absl::Hash<model::topic_id_partition>{}(tidp);
    }
};

struct log_maintenance_meta_eq {
    using is_transparent = void;

    bool operator()(
      const cloud_topics::l1::log_maintenance_meta_ptr& lhs,
      const cloud_topics::l1::log_maintenance_meta_ptr& rhs) const {
        return lhs->tidp == rhs->tidp;
    }

    bool operator()(
      const cloud_topics::l1::log_maintenance_meta_ptr& lhs,
      const model::topic_id_partition& rhs) const noexcept {
        return lhs->tidp == rhs;
    }

    bool operator()(
      const model::topic_id_partition& lhs,
      const cloud_topics::l1::log_maintenance_meta_ptr& rhs) const {
        return lhs == rhs->tidp;
    }
};

using log_set_t = chunked_hash_set<
  log_maintenance_meta_ptr,
  log_maintenance_meta_hash,
  log_maintenance_meta_eq>;

using log_list_t
  = intrusive_list<log_maintenance_meta, &log_maintenance_meta::link>;

using cmp_t = std::function<bool(
  const log_maintenance_meta_ptr&, const log_maintenance_meta_ptr&)>;
using log_maintenance_queue = std::priority_queue<
  log_maintenance_meta_ptr,
  chunked_vector<log_maintenance_meta_ptr>,
  cmp_t>;

enum class maintenance_job_state {
    // No compaction job is currently inflight.
    idle,
    // A compaction job is currently inflight.
    running,
    // A graceful stop has been requested of an inflight compaction job.
    // The user should try to commit as much useful data as possible while still
    // shutting down in a prompt manner.
    soft_stop,
    // A forceful stop has been requested of an inflight compaction job.
    // The user should abandon any work and shutdown immediately.
    hard_stop
};

inline fmt::iterator format_to(maintenance_job_state s, fmt::iterator out) {
    switch (s) {
    case maintenance_job_state::idle:
        return fmt::format_to(out, "idle");
    case maintenance_job_state::running:
        return fmt::format_to(out, "running");
    case maintenance_job_state::soft_stop:
        return fmt::format_to(out, "soft_stop");
    case maintenance_job_state::hard_stop:
        return fmt::format_to(out, "hard_stop");
    }
}

} // namespace cloud_topics::l1
