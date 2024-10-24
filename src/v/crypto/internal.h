/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "bytes/bytes.h"
#include "ssl_utils.h"

namespace crypto::internal {
inline bytes_view string_view_to_bytes_view(std::string_view v) {
    return {// NOLINTNEXTLINE: allow reinterpret_cast
            reinterpret_cast<const bytes_view::value_type*>(v.data()),
            v.size()};
}

inline bytes_span<> char_span_to_bytes_span(std::span<char> v) {
    // NOLINTNEXTLINE: allow reinterpret_cast
    return {reinterpret_cast<bytes_span<>::value_type*>(v.data()), v.size()};
}

EVP_MD* get_md(digest_type type);
EVP_MAC* get_mac();
bool fips_enabled();
} // namespace crypto::internal
