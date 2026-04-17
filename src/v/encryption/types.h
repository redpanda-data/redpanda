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
#include "bytes/bytes.h"
#include "container/chunked_hash_map.h"
#include "model/timestamp.h"

#include <seastar/core/sstring.hh>

#include <cstdint>
#include <optional>

namespace encryption {

enum class dek_algorithm {
    aes128_gcm,
    aes256_gcm,
    aes256_siv,
};

struct dek_state {
    bytes plaintext_dek;
    bytes encrypted_dek;
    dek_algorithm algorithm;
    ss::sstring kek_name;
    ss::sstring kms_type;
    ss::sstring kms_key_id;
    uint32_t version;
    model::timestamp created_at;
    std::optional<model::timestamp> expiry;
};

/// Map from kek_name to dek_state, used per-batch
using dek_set = chunked_hash_map<ss::sstring, dek_state>;

} // namespace encryption
