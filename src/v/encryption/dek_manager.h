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

#include "absl/hash/hash.h"
#include "base/seastarx.h"
#include "container/chunked_hash_map.h"
#include "encryption/kms_provider.h"
#include "encryption/types.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <chrono>
#include <cstdint>
#include <optional>
#include <string_view>
#include <utility>

namespace encryption {

/// \brief Per-shard service that generates, wraps, caches, and tracks DEKs.
///
/// Each (subject, kek_name) pair gets its own active DEK with independent
/// version numbering. DEKs are rotated when they expire (if an expiry is
/// configured).
class dek_manager {
public:
    explicit dek_manager(kms_provider& kms);

    /// Get or create the active DEK for (subject, kek_name).
    /// Generates a new DEK on first call or after expiry.
    ss::future<dek_state> get_or_create_dek(
      ss::sstring subject,
      ss::sstring kek_name,
      ss::sstring kms_type,
      ss::sstring kms_key_id,
      dek_algorithm algo,
      std::optional<std::chrono::seconds> expiry);

private:
    struct cache_key_hash {
        size_t
        operator()(const std::pair<ss::sstring, ss::sstring>& key) const {
            return absl::HashOf(
              std::string_view(key.first), std::string_view(key.second));
        }
    };

    using cache_key = std::pair<ss::sstring, ss::sstring>;
    using cache_map = chunked_hash_map<cache_key, dek_state, cache_key_hash>;

    ss::future<dek_state> generate_dek(
      ss::sstring kek_name,
      ss::sstring kms_type,
      ss::sstring kms_key_id,
      dek_algorithm algo,
      uint32_t version);

    kms_provider& _kms;
    cache_map _active_deks;
};

} // namespace encryption
