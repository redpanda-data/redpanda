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

#include "crypto/types.h"
#include "internal.h"

#include <absl/container/node_hash_map.h>
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

std::ostream& operator<<(std::ostream& os, key_type type) {
    switch (type) {
    case key_type::RSA:
        return os << "RSA";
    }
}

std::ostream& operator<<(std::ostream& os, format_type type) {
    switch (type) {
    case format_type::PEM:
        return os << "PEM";
    case format_type::DER:
        return os << "DER";
    }
}

namespace internal {
EVP_MD* get_md(digest_type type) {
    // Map of pre-fetched MD pointers.  This replaces the older way of getting
    // a digest pointer via something like `EVP_sha256()`.  The old way is
    // slower compared to pre-fetching the algorithm.
    static thread_local absl::node_hash_map<digest_type, EVP_MD_ptr> md_map;
    auto it = md_map.find(type);
    if (it == md_map.end()) {
        auto alg = fmt::to_string(type);
        auto md_ptr = EVP_MD_fetch(nullptr, alg.c_str(), "?provider=fips");
        if (!md_ptr) {
            throw ossl_error(fmt::format("Failed to fetch algorithm {}", alg));
        }
        auto res = md_map.insert_or_assign(type, EVP_MD_ptr(md_ptr));
        vassert(
          res.second, "Failed to insert/create the fetched algorithm {}", alg);
        return md_ptr;
    }

    return it->second.get();
}

EVP_MAC* get_mac() {
    static thread_local EVP_MAC_ptr mac;
    if (!mac) {
        mac = EVP_MAC_ptr(EVP_MAC_fetch(nullptr, "HMAC", "?provider=fips"));
        if (!mac) {
            throw ossl_error("Failed to fetch HMAC algorithm");
        }
    }
    return mac.get();
}
} // namespace internal
} // namespace crypto
