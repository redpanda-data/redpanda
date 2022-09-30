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

#include "cluster/internal_secret.h"

#include <absl/container/flat_hash_map.h>

#include <iosfwd>

namespace cluster {

class internal_secret_store {
public:
    using key_t = internal_secret::key_t;
    using value_t = internal_secret::value_t;

    auto insert_or_assign(const internal_secret& s) {
        return _store.insert_or_assign(s.key(), s.value()).second;
    }

    std::optional<value_t> get(const key_t& k) {
        auto it = _store.find(k);
        return it != _store.end() ? std::optional<value_t>{it->second}
                                  : std::nullopt;
    }

private:
    using store_t = absl::flat_hash_map<key_t, value_t>;
    store_t _store;
};

} // namespace cluster
