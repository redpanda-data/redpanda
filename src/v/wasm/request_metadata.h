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

#include <seastar/core/sstring.hh>

#include <cstdint>

namespace wasm {

/// Metadata from the request context available during produce-path
/// transforms. All fields are empty/zero when not available (e.g.
/// sidecar mode).
struct request_metadata {
    ss::sstring principal_name;
    ss::sstring principal_type;
    ss::sstring client_id;
    ss::sstring client_host;
    uint16_t client_port{0};
    bool tls_enabled{false};
};

/// Well-known keys for the read_batch_metadata ABI function.
enum class metadata_key : int32_t {
    principal_name = 1,
    principal_type = 2,
    client_id = 3,
    client_host = 4,
    client_port = 5,
    tls_enabled = 6,
};

} // namespace wasm
