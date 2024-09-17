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
#pragma once

#include "security/ephemeral_credential.h"

#include <absl/container/flat_hash_set.h>

#include <functional>
#include <iosfwd>

namespace security {

/*
 * Store for ephemerable user credentials.
 */
class ephemeral_credential_store {
    using value_type = ephemeral_credential;

    struct get_principal_ref {
        const acl_principal& operator()(const acl_principal& p) { return p; }
        const acl_principal& operator()(const value_type& v) {
            return v.principal();
        }
    };

    struct hasher {
        using is_transparent = void;
        template<typename T>
        size_t operator()(const T& t) const {
            return absl::Hash<acl_principal>()(get_principal_ref{}(t));
        };
    };

    struct hash_comp {
        using is_transparent = void;
        template<typename L, typename R>
        bool operator()(const L& l, const R& r) const {
            return std::equal_to<>()(
              get_principal_ref{}(l), get_principal_ref{}(r));
        }
    };

    using underlying_t = absl::flat_hash_set<value_type, hasher, hash_comp>;
    using const_iterator = underlying_t::const_iterator;

public:
    ephemeral_credential_store() = default;

    const_iterator find(const acl_principal& p) const {
        assert_principal(p);
        return _credentials.find(p);
    }

    bool has(const_iterator it) const { return it != _credentials.end(); }

    const_iterator insert_or_assign(value_type s) {
        assert_principal(s.principal());
        auto it = _credentials.find(s);
        if (!has(it)) {
            return _credentials.insert(std::move(s)).first;
        }
        auto n = _credentials.extract(it);
        n.value() = std::move(s);
        return _credentials.insert(std::move(n)).position;
    }

private:
    static void assert_principal(const acl_principal& p) {
        vassert(
          p.type() == principal_type::ephemeral_user,
          "principal_type expected to be ephemeral: {}",
          p);
    }

    underlying_t _credentials;
};

} // namespace security
