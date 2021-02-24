/*
 * Copyright 2020 Vectorized, Inc.
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
#include "kafka/security/scram_credential.h"
#include "utils/concepts-enabled.h"

#include <absl/container/node_hash_map.h>

namespace kafka {

/*
 * Store for user credentials.
 *
 * Credentials can be copied or moved into the store. Reads always return a copy
 * since the typical use case is that a credential is used in an authentication
 * process that often spans multiple network round trips and should remain
 * consistent for the duration of that process.
 */
class credential_store {
public:
    credential_store() noexcept = default;
    credential_store(const credential_store&) = delete;
    credential_store& operator=(const credential_store&) = delete;
    credential_store(credential_store&&) noexcept = default;
    credential_store& operator=(credential_store&&) noexcept = default;
    ~credential_store() noexcept = default;

    template<typename T>
    void put(const ss::sstring& name, T&& credential) {
        _credentials.insert_or_assign(name, std::forward<T>(credential));
    }

    template<typename T>
    auto get(const ss::sstring& name) -> std::optional<T> const {
        if (auto it = _credentials.find(name); it != _credentials.end()) {
            return std::get<T>(it->second);
        }
        return std::nullopt;
    }

private:
    // when a second type is supported update `credential_store_test` to include
    // a mismatched credential type test for get<type>(name).
    using credential_types = std::variant<scram_credential>;

    absl::node_hash_map<ss::sstring, credential_types> _credentials;
};

} // namespace kafka
