/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/types.h"
#include "feature_state.h"
#include "model/fundamental.h"
#include "security/license.h"
#include "serde/serde.h"

namespace features {

class feature_table;

/**
 * Part of feature_table_snapshot that corresponds to the feature_state
 * stored within feature_table.
 *
 * Unlike, feature_state, this class embeds the feature name directly for
 * serialization, rather than encapsulating a reference to a feature_spec.
 */
struct feature_state_snapshot
  : serde::envelope<
      feature_state_snapshot,
      serde::version<0>,
      serde::compat_version<0>> {
    ss::sstring name;
    feature_state::state state;

    auto serde_fields() { return std::tie(name, state); }
};

/**
 * Rather than adding serialization methods to feature_table that would
 * include some subset of its fields, define a clean separate serialization
 * containers for when encoding or decoding a snapshot.
 */
struct feature_table_snapshot
  : serde::envelope<
      feature_table_snapshot,
      serde::version<1>,
      serde::compat_version<0>> {
    model::offset applied_offset;
    cluster::cluster_version version{cluster::invalid_version};
    std::optional<security::license> license;
    std::vector<feature_state_snapshot> states;
    cluster::cluster_version original_version{cluster::invalid_version};

    auto serde_fields() {
        return std::tie(
          applied_offset, version, states, license, original_version);
    }

    /// Create a snapshot from a live feature table
    static feature_table_snapshot from(const feature_table&);

    /// Replace the feature table's state with this snapshot
    void apply(feature_table&) const;

    /// Key for storing the snapshot in the shard 0 kvstore.
    static bytes kvstore_key();
};

} // namespace features
