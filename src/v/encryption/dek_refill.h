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
#include "bytes/iobuf.h"
#include "model/record.h"

#include <seastar/core/future.hh>

#include <optional>

namespace encryption {

struct refill_result {
    model::record_batch batch;
    std::optional<iobuf> last_dek_metadata;
};

/// Refill sentinel rp.encryption headers within a single batch.
///
/// Reads the first record's rp.encryption header value. For every
/// subsequent record that has rp.encryption with an empty value
/// (sentinel), replaces it with the first record's value.
///
/// If the first record's header is also a sentinel or absent,
/// uses the provided fallback value (from a previous batch).
///
/// Returns the refilled batch and the last seen full header value.
ss::future<refill_result> refill_dek_sentinels(
  model::record_batch batch, std::optional<iobuf> previous_dek_metadata);

} // namespace encryption
