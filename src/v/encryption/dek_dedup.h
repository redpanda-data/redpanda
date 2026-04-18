/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"
#include "container/chunked_hash_map.h"
#include "model/record.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <cstdint>

namespace encryption {

/// Identifies a unique DEK by KEK name and DEK version.
struct dek_id {
    ss::sstring kek_name;
    uint32_t dek_version;

    bool operator==(const dek_id&) const = default;

    template<typename H>
    friend H AbslHashValue(H h, const dek_id& id) {
        return H::combine(std::move(h), id.kek_name, id.dek_version);
    }
};

using seen_dek_set = chunked_hash_set<dek_id>;

/// Strip duplicate DEK entries from a batch's rp.encryption headers.
///
/// For each record with an rp.encryption header, DEK entries whose
/// (kek_name, dek_version) pair already appears in seen_deks are removed.
/// Newly encountered pairs are added to seen_deks. If all DEK entries in
/// a record's header are duplicates, the header is removed entirely.
/// The batch is rebuilt only when modifications are made.
ss::future<model::record_batch>
strip_duplicate_dek_headers(model::record_batch batch, seen_dek_set& seen_deks);

} // namespace encryption
