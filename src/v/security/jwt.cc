/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "security/jwt.h"

namespace security::oidc::detail {
bytes base64_url_decode(std::string_view sv) { return base64url_to_bytes(sv); };

std::optional<bytes>
base64_url_decode(const json::Value& v, std::string_view field) {
    auto b64 = string_view<>(v, field);
    if (!b64.has_value()) {
        return std::nullopt;
    }
    return base64_url_decode(b64.value());
}
} // namespace security::oidc::detail
