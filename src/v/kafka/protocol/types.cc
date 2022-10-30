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
#include "kafka/protocol/types.h"

#include "utils/base64.h"

namespace kafka {

uuid uuid::from_string(std::string_view encoded) {
    if (encoded.size() > 24) {
        details::throw_out_of_range(
          "Input size of {} too long to be decoded as b64-UUID, expected "
          "{} bytes or less",
          encoded.size(),
          24);
    }
    auto decoded = base64_to_bytes(encoded);
    if (decoded.size() != length) {
        details::throw_out_of_range(
          "Expected {} byte value post b64decoding the input: {} bytes",
          length,
          decoded.size());
    }
    underlying_t ul;
    std::copy_n(decoded.begin(), length, ul.begin());
    return uuid(ul);
}

ss::sstring uuid::to_string() const { return bytes_to_base64(view()); }

std::ostream& operator<<(std::ostream& os, const uuid& u) {
    return os << u.to_string();
}

} // namespace kafka
