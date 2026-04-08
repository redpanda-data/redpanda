/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/seastarx.h"
#include "cloud_topics/level_one/metastore/domain_uuid.h"
#include "container/chunked_vector.h"
#include "serde/envelope.h"
#include "serde/rw/envelope.h"

#include <seastar/core/future.hh>

namespace cloud_topics::l1 {

// Contains metadata about the metastore topic. With this metadata, can be used
// to drive restoration of the metastore.
struct metastore_manifest
  : public serde::envelope<
      metastore_manifest,
      serde::version<0>,
      serde::compat_version<0>> {
    friend bool
    operator==(const metastore_manifest&, const metastore_manifest&) = default;
    auto serde_fields() { return std::tie(partitioning_strategy, domains); }

    // TODO: in the future, this may have a fancier mapping.
    ss::sstring partitioning_strategy = "murmur";

    // The domain UUIDs will prefix all metadata within a given domain.
    chunked_vector<domain_uuid> domains;
};

} // namespace cloud_topics::l1
