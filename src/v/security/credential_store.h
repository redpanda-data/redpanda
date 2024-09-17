/*
 * Copyright 2020 Redpanda Data, Inc.
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
#include "security/scram_credential.h"
#include "security/types.h"

#include <seastar/util/variant_utils.hh>

#include <absl/container/node_hash_map.h>
#include <boost/range/adaptor/filtered.hpp>

namespace security {

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
    // when a second type is supported update `credential_store_test` to include
    // a mismatched credential type test for get<type>(name).
    using credential_types = std::variant<scram_credential>;

    using container_type
      = absl::node_hash_map<credential_user, credential_types>;
    using const_iterator = container_type::const_iterator;

    credential_store() noexcept = default;
    credential_store(const credential_store&) = delete;
    credential_store& operator=(const credential_store&) = delete;
    credential_store(credential_store&&) noexcept = default;
    credential_store& operator=(credential_store&&) noexcept = default;
    ~credential_store() noexcept = default;

    template<typename T>
    void put(const credential_user& name, T&& credential) {
        _credentials.insert_or_assign(name, std::forward<T>(credential));
    }

    template<typename T>
    auto get(const credential_user& name) const -> const std::optional<T> {
        if (auto it = _credentials.find(name); it != _credentials.end()) {
            return std::get<T>(it->second);
        }
        return std::nullopt;
    }

    bool remove(const credential_user& user) {
        return _credentials.erase(user) > 0;
    }

    bool contains(const credential_user& name) const {
        return _credentials.contains(name);
    }

    // Ephemeral credentials often require careful handling; they must not
    // be serialized to disk, and in the general case, should not be displayed
    // to users.
    static constexpr auto is_not_ephemeral =
      [](const security::credential_store::container_type::value_type& t) {
          return ss::visit(t.second, [](const security::scram_credential& c) {
              return !c.principal().has_value()
                     || c.principal().value().type()
                          != security::principal_type::ephemeral_user;
          });
      };

    // Retrieve a list of credentials that satisfy the predicate.
    //
    // E.g.:
    // _creds.range(credential_store::is_not_ephemeral);
    auto range(auto pred) {
        return boost::adaptors::filter(_credentials, std::move(pred));
    }
    void clear() { _credentials.clear(); }

private:
    container_type _credentials;
};

} // namespace security
