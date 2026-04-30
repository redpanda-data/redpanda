/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "base/seastarx.h"
#include "container/chunked_vector.h"
#include "impl/types.h"
#include "serde/envelope.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sstring.hh>

#include <sys/types.h>

#include <functional>
#include <tuple>

namespace cluster::sloth_mail::kinds {
/*
 * Mail kinds supported in the app (as opposed to tests).
 *
 * To introduce a new mail kind:
 * 1) add new struct here;
 * 2) implement hashing for the key type below;
 * 3) add the new struct to the @supported_kinds variant futher below
 */
struct dummy_0 {
    static constexpr kind_id id{0};

    struct key_t
      : public serde::
          envelope<key_t, serde::version<0>, serde::compat_version<0>> {
        int key;

        friend bool operator==(const key_t&, const key_t&) = default;

        auto serde_fields() { return std::tie(key); }
    };

    struct value_t
      : public serde::
          envelope<value_t, serde::version<0>, serde::compat_version<0>> {
        ss::sstring value;

        friend bool operator==(const value_t&, const value_t&) = default;

        auto serde_fields() { return std::tie(value); }
    };

    static void process(chunked_vector<std::pair<key_t, value_t>>&&) {}

    static void merge(const key_t&, value_t& value, const value_t& new_value) {
        value.value += "+" + new_value.value;
    }

    constexpr static size_t entry_size = sizeof(key_t) + sizeof(value_t);
};
} // namespace cluster::sloth_mail::kinds

namespace std {
template<>
struct hash<cluster::sloth_mail::kinds::dummy_0::key_t> {
    size_t
    operator()(const cluster::sloth_mail::kinds::dummy_0::key_t& k) const {
        return std::hash<int>()(k.key);
    }
};

} // namespace std

namespace cluster::sloth_mail {
// for easier duplicate detection must be in the order of kind_id values
using supported_kinds = std::variant<kinds::dummy_0>;

} // namespace cluster::sloth_mail
