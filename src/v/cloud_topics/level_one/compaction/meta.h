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

#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/metastore/metastore.h"
#include "cloud_topics/level_one/metastore/offset_interval_set.h"
#include "container/chunked_hash_map.h"
#include "container/intrusive_list_helpers.h"
#include "model/fundamental.h"
#include "model/timestamp.h"

#include <seastar/core/gate.hh>

#include <memory>

namespace cloud_topics::l1 {

struct log_compaction_meta {
    log_compaction_meta(model::topic_id_partition tid_p, model::ntp ntp)
      : tid_p(std::move(tid_p))
      , ntp(std::move(ntp)) {}

    model::topic_id_partition tid_p;
    model::ntp ntp;
    ss::gate gate;
    intrusive_list_hook link;
};

using log_compaction_meta_ptr = std::unique_ptr<log_compaction_meta>;

struct log_compaction_meta_hash {
    using is_transparent = void;

    size_t
    operator()(const cloud_topics::l1::log_compaction_meta_ptr& m) const {
        return std::hash<model::ntp>{}(m->ntp);
    }

    size_t operator()(const model::ntp& ntp) const {
        return std::hash<model::ntp>{}(ntp);
    }
};

struct log_compaction_meta_eq {
    using is_transparent = void;

    bool operator()(
      const cloud_topics::l1::log_compaction_meta_ptr& lhs,
      const cloud_topics::l1::log_compaction_meta_ptr& rhs) const {
        return lhs->ntp == rhs->ntp;
    }

    bool operator()(
      const cloud_topics::l1::log_compaction_meta_ptr& lhs,
      const model::ntp& rhs) const noexcept {
        return lhs->ntp == rhs;
    }

    bool operator()(
      const model::ntp& lhs,
      const cloud_topics::l1::log_compaction_meta_ptr& rhs) const {
        return lhs == rhs->ntp;
    }
};

using logs_type_t = chunked_hash_set<
  log_compaction_meta_ptr,
  log_compaction_meta_hash,
  log_compaction_meta_eq>;

using log_list_t
  = intrusive_list<log_compaction_meta, &log_compaction_meta::link>;

struct log_info_and_meta {
    metastore::compaction_info_response info;
    log_compaction_meta* meta;
};

// Represents the output from a compaction job over a cloud topic partition.
// Highly subject to change in the future.
struct object_output_t {
    metastore::object_metadata::ntp_metadata ntp_md;
    object_builder::object_info info;
    std::unique_ptr<staging_file> staging_file;
};

} // namespace cloud_topics::l1
