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
#include "thirdparty/openssl/evp.h"
#include "thirdparty/openssl/provider.h"

#include <absl/container/flat_hash_set.h>
#include <absl/container/node_hash_map.h>

#include <sstream>
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
// This function returns true if the provided digest type is expected to use the
// default provider regardless if the application is in FIPS mode or not.  The
// use of said digests are not necessarily disallowed, as long as the operations
// being performed are not security relevant (e.g. performing a checksum rather
// than an HMAC)
bool uses_default_provider(digest_type type) {
    switch (type) {
    case digest_type::MD5:
        return true;
    case digest_type::SHA256:
    case digest_type::SHA512:
        return false;
    }
}
bool uses_default_provider(const EVP_MD* md) {
    struct callback_func_data {
        bool digest_type_set = false;
        // type is not valid unless digest_type_set is true
        digest_type type = digest_type::MD5;
    } data;

    const auto fn = [](const char* name, void* vdata) {
        auto data = static_cast<callback_func_data*>(vdata);
        if (!data->digest_type_set) {
            std::string name_str{name};
            if (name_str == "MD5") {
                data->type = digest_type::MD5;
                data->digest_type_set = true;
            } else if (name_str == "SHA256") {
                data->type = digest_type::SHA256;
                data->digest_type_set = true;
            } else if (name_str == "SHA512") {
                data->type = digest_type::SHA512;
                data->digest_type_set = true;
            }
        }
    };

    EVP_MD_names_do_all(md, fn, &data);

    vassert(data.digest_type_set, "Encountered unknown digest type");

    return uses_default_provider(data.type);
}
// Validates that the message digest is using the correct provider when in FIPS
// mode.  This function will assert if all of:
// 1. The application is in FIPS mode
// 2. The message digest algorithm is expected to use the FIPS module, and
// 3. The message digest algorithm is NOT using the FIPS module
void validate_provider(const EVP_MD* md) {
    if (!fips_enabled()) {
        return;
    }
    auto provider_name = OSSL_PROVIDER_get0_name(EVP_MD_get0_provider(md));
    if (!uses_default_provider(md)) {
        vassert(
          provider_name == std::string("fips"),
          "Expected to use FIPS provider but found {}",
          provider_name);
    }
}
std::string get_property_string(digest_type type) {
    std::stringstream property_string;
    if (uses_default_provider(type)) {
        // This will negate `fips=on` that is set in initialization when
        // Redpanda is in FIPS mode
        property_string << "-fips,";
    }
    // This informs the fetch to prefer the FIPS module if present.  If it
    // isn't, it will use the provider that has the implementation for the
    // requested digest type (if present).
    property_string << "?provider=fips";
    return property_string.str();
}
EVP_MD* get_md(digest_type type) {
    // Map of pre-fetched MD pointers.  This replaces the older way of getting
    // a digest pointer via something like `EVP_sha256()`.  The old way is
    // slower compared to pre-fetching the algorithm.
    static thread_local absl::node_hash_map<digest_type, EVP_MD_ptr> md_map;
    auto it = md_map.find(type);
    if (it == md_map.end()) {
        auto alg = fmt::to_string(type);
        auto md_ptr = EVP_MD_fetch(
          nullptr, alg.c_str(), get_property_string(type).c_str());
        if (!md_ptr) {
            throw ossl_error(fmt::format("Failed to fetch algorithm {}", alg));
        }
        validate_provider(md_ptr);
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

bool fips_enabled() { return 1 == OSSL_PROVIDER_available(nullptr, "fips"); }
} // namespace internal
} // namespace crypto
