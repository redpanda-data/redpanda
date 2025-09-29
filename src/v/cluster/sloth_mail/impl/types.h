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
#include "cluster/errc.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "serde/envelope.h"
#include "utils/named_type.h"
#include "utils/variant.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sstring.hh>

#include <boost/container_hash/hash.hpp>
#include <sys/types.h>

#include <algorithm>
#include <cstdint>
#include <functional>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>

namespace cluster::sloth_mail {

using deadline_t = seastar::lowres_clock::time_point;

using kind_id = named_type<int32_t, struct kind_id_tag>;

template<typename Kind>
concept mail_kind
  = serde::is_envelope<typename Kind::key_t>
    && serde::is_envelope<typename Kind::value_t>
    && requires(typename Kind::key_t key, typename Kind::value_t value) {
           { Kind::id } -> std::same_as<const kind_id&>;
           {
               Kind::merge(std::as_const(key), value, std::as_const(value))
           } -> std::same_as<void>;
           { Kind::entry_size } -> std::same_as<const size_t&>;
           std::hash<typename Kind::key_t>{};
       };

namespace detail {
template<typename Kinds>
class kind_id_utils {
    template<typename... Kind>
    static constexpr std::array<kind_id, sizeof...(Kind)>
    generate_kind_ids_array(std::type_identity<std::variant<Kind...>>) {
        return {Kind::id...};
    };

public:
    static constexpr auto kinds_ids_array = generate_kind_ids_array(
      std::type_identity<Kinds>{});

private:
    static_assert(
      std::ranges::adjacent_find(kinds_ids_array, std::greater_equal<>())
        == kinds_ids_array.end(),
      "Mail kind ids must be distinct and sorted");
};

// define once for these types to be back and forward compatible when new
// kinds are introduced
template<mail_kind Kind>
struct pair_of_kind
  : serde::envelope<
      pair_of_kind<Kind>,
      serde::version<0>,
      serde::compat_version<0>> {
    typename Kind::key_t key;
    typename Kind::value_t value;
    auto serde_fields() { return std::tie(key, value); }
    friend bool operator==(const pair_of_kind&, const pair_of_kind&) = default;
};

} // namespace detail

template<typename Kinds>
concept mail_kinds = requires { typename detail::kind_id_utils<Kinds>; };

namespace impl {

template<mail_kinds Kinds>
class types {
    template<typename Kind>
    using key_of_kind = typename Kind::key_t;

    template<typename Kind>
    using value_of_kind = typename Kind::value_t;

    template<typename Kind>
    using pair_of_kind = detail::pair_of_kind<Kind>;

public:
    template<typename Kind>
    using pairs_vector_of_kind = chunked_vector<pair_of_kind<Kind>>;

    template<mail_kind Kind>
    struct tagged_key_of_kind {
        using kind = Kind;
        using kinds = Kinds;
        using vector_of_pairs_t = pairs_vector_of_kind<Kind>;
        typename Kind::key_t key;

        friend bool
        operator==(const tagged_key_of_kind&, const tagged_key_of_kind&)
          = default;
    };

private:
    using any_tagged_key
      = map_and_reassemble_variant<Kinds, std::variant, tagged_key_of_kind>;
    using any_value
      = map_and_reassemble_variant<Kinds, std::variant, value_of_kind>;
    using any_pairs_vector
      = map_and_reassemble_variant<Kinds, std::variant, pairs_vector_of_kind>;

    constexpr static auto kinds_ids_array
      = detail::kind_id_utils<Kinds>::kinds_ids_array;

    // returns ids_array.size() if not found
    constexpr static size_t variant_index_by_kind_id(kind_id id) {
        return std::ranges::find(kinds_ids_array, id) - kinds_ids_array.begin();
    }

public:
    using kind_map_t = chunked_hash_map<kind_id, any_pairs_vector>;
    using kv_map_t = chunked_hash_map<any_tagged_key, any_value>;

    struct mail_request
      : serde::
          envelope<mail_request, serde::version<0>, serde::compat_version<0>> {
        kind_map_t data;

        mail_request copy() const {
            mail_request r;
            r.data.reserve(data.size());
            for (auto& [k, v] : data) {
                std::visit(
                  [&k, &r](const auto& v) {
                      using vec_t = std::decay_t<decltype(v)>;
                      r.data.emplace(k, vec_t{std::from_range, v});
                  },
                  v);
            }
            return r;
        }

        auto serde_fields() { return std::tie(data); }
        friend bool operator==(const mail_request&, const mail_request&)
          = default;
        friend std::ostream& operator<<(std::ostream&, const mail_request&);
    };
};

struct mail_reply
  : serde::envelope<mail_reply, serde::version<0>, serde::compat_version<0>> {
    cluster::errc ec{cluster::errc::success};
    auto serde_fields() { return std::tie(ec); }
    friend bool operator==(const mail_reply&, const mail_reply&) = default;
    friend std::ostream& operator<<(std::ostream&, const mail_reply&);
};

} // namespace impl

template<typename Config>
concept mail_config
  = mail_kinds<typename Config::supported_kinds>
    && requires(
      Config c,
      model::node_id destination,
      impl::types<typename Config::supported_kinds>::mail_request&& req) {
           {
               c.do_ship_mail(destination, std::move(req))
           } -> std::same_as<seastar::future<impl::mail_reply>>;
       };

} // namespace cluster::sloth_mail

namespace std {
template<typename TaggedKeyOfKind>
requires cluster::sloth_mail::mail_kind<typename TaggedKeyOfKind::kind>
         && cluster::sloth_mail::mail_kinds<typename TaggedKeyOfKind::kinds>
struct hash<TaggedKeyOfKind> {
    constexpr size_t operator()(const TaggedKeyOfKind& tkk) const {
        auto h = hash<cluster::sloth_mail::kind_id::type>()(
          TaggedKeyOfKind::kind::id());
        boost::hash_combine(
          h, hash<std::decay_t<decltype(tkk.key)>>()(tkk.key));
        return h;
    }
};
} // namespace std
