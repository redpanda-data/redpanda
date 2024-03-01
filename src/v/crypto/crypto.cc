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

#include "crypto/crypto.h"

namespace crypto {
std::ostream& operator<<(std::ostream& os, digest_type type) {
    switch (type) {
    case digest_type::MD5:
        return os << "MD5";
    case digest_type::SHA256:
        return os << "SHA256";
    case digest_type::SHA512:
        return os << "SHA512";
    }

    return os;
}
} // namespace crypto
