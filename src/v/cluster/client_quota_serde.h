
// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "base/seastarx.h"
#include "cluster/errc.h"
#include "model/timeout_clock.h"
#include "seastar/core/sstring.hh"
#include "serde/envelope.h"
#include "serde/rw/variant.h"
#include "strings/string_switch.h"

#include <absl/container/flat_hash_set.h>

#include <concepts>
#include <cstdint>
#include <iosfwd>
#include <vector>

namespace cluster::client_quota {

/// entity_key is used to key client quotas. It consists of multiple parts as a
/// key can be a combination of key parts. Currently, only client id based and
/// Redpanda-specific client id prefix-based quotas are supported, so all entity
/// keys will consist of a single part, but in the future if we extend to client
/// id and user principal based quotas, the entity key can contain two parts.
struct entity_key
  : serde::envelope<entity_key, serde::version<0>, serde::compat_version<0>> {
private:
    template<typename T>
    struct constructor : public T {
        constructor() = default;
        template<typename U>
        requires std::constructible_from<ss::sstring, U>
        explicit constructor(U&& u)
          : T{.value{std::forward<U>(u)}} {}
    };

public:
    struct part
      : serde::envelope<part, serde::version<0>, serde::compat_version<0>> {
        friend bool operator==(const part&, const part&) = default;
        friend std::ostream& operator<<(std::ostream&, const part&);

        template<typename H>
        friend H AbslHashValue(H h, const part& e) {
            return H::combine(std::move(h), e.part);
        }

        /// client_id_default_match is the quota entity type corresponding to
        /// /config/clients/<default>
        struct client_id_default_match
          : serde::envelope<
              client_id_default_match,
              serde::version<0>,
              serde::compat_version<0>> {
            friend bool operator==(
              const client_id_default_match&, const client_id_default_match&)
              = default;

            auto serde_fields() { return std::tie(); }

            friend std::ostream&
            operator<<(std::ostream&, const client_id_default_match&);

            template<typename H>
            friend H AbslHashValue(H h, const client_id_default_match&) {
                return H::combine(
                  std::move(h), typeid(client_id_default_match).hash_code());
            }
        };

        /// client_id_match is the quota entity type corresponding to
        /// /config/clients/<client-id>
        struct client_id_match
          : serde::envelope<
              client_id_match,
              serde::version<0>,
              serde::compat_version<0>> {
            friend bool
            operator==(const client_id_match&, const client_id_match&)
              = default;

            friend std::ostream&
            operator<<(std::ostream&, const client_id_match&);

            template<typename H>
            friend H AbslHashValue(H h, const client_id_match& c) {
                return H::combine(
                  std::move(h), typeid(client_id_match).hash_code(), c.value);
            }

            ss::sstring value;

            auto serde_fields() { return std::tie(value); }
        };

        /// client_id_prefix_match is the quota entity type corresponding to the
        /// Redpanda-specific client prefix match
        /// /config/client-id-prefix/<client-id-prefix>
        struct client_id_prefix_match
          : serde::envelope<
              client_id_prefix_match,
              serde::version<0>,
              serde::compat_version<0>> {
            friend bool operator==(
              const client_id_prefix_match&, const client_id_prefix_match&)
              = default;

            friend std::ostream&
            operator<<(std::ostream&, const client_id_prefix_match&);

            template<typename H>
            friend H AbslHashValue(H h, const client_id_prefix_match& c) {
                return H::combine(
                  std::move(h),
                  typeid(client_id_prefix_match).hash_code(),
                  c.value);
            }

            ss::sstring value;
            auto serde_fields() { return std::tie(value); }
        };

        auto serde_fields() { return std::tie(part); }

        serde::variant<
          client_id_default_match,
          client_id_match,
          client_id_prefix_match>
          part;
    };

    using client_id_default_match = constructor<part::client_id_default_match>;
    using client_id_match = constructor<part::client_id_match>;
    using client_id_prefix_match = constructor<part::client_id_prefix_match>;

    template<typename... T>
    explicit entity_key(T&&... t)
      : parts{{.part{std::forward<T>(t)}}...} {}

    auto serde_fields() { return std::tie(parts); }

    friend bool operator==(const entity_key&, const entity_key&) = default;
    friend std::ostream& operator<<(std::ostream&, const entity_key&);

    template<typename H>
    friend H AbslHashValue(H h, const entity_key& e) {
        return AbslHashValue(std::move(h), e.parts);
    }

    absl::flat_hash_set<part> parts;
};

/// entity_value describes the quotas applicable to an entity_key
struct entity_value
  : serde::envelope<entity_value, serde::version<0>, serde::compat_version<0>> {
    friend bool operator==(const entity_value&, const entity_value&) = default;
    friend std::ostream& operator<<(std::ostream&, const entity_value&);

    bool is_empty() const {
        return !producer_byte_rate && !consumer_byte_rate
               && !controller_mutation_rate;
    }

    std::optional<uint64_t> producer_byte_rate;
    std::optional<uint64_t> consumer_byte_rate;
    std::optional<uint64_t> controller_mutation_rate;

    auto serde_fields() {
        return std::tie(
          producer_byte_rate, consumer_byte_rate, controller_mutation_rate);
    }
};

/// entity_value_diff describes the quotas diff for an entity_key
struct entity_value_diff
  : serde::
      envelope<entity_value_diff, serde::version<0>, serde::compat_version<0>> {
    enum class key {
        producer_byte_rate = 0,
        consumer_byte_rate,
        controller_mutation_rate,
    };

    enum class operation {
        upsert = 0,
        remove,
    };

    struct entry
      : serde::envelope<entry, serde::version<0>, serde::compat_version<0>> {
        constexpr entry() noexcept = default;
        constexpr entry(operation op, key type, uint64_t value) noexcept
          : op(op)
          , type(type)
          , value(value) {}
        constexpr entry(key type, uint64_t value) noexcept
          : entry(operation::upsert, type, value) {}

        // Custom equality to match the hash function
        friend bool operator==(const entry&, const entry&);
        friend std::ostream& operator<<(std::ostream&, const entry&);

        constexpr auto serde_fields() { return std::tie(op, type, value); }

        template<typename H>
        constexpr friend H AbslHashValue(H h, const entry& e) {
            switch (e.op) {
            case entity_value_diff::operation::upsert:
                return H::combine(std::move(h), e.op, e.type, e.value);
            case entity_value_diff::operation::remove:
                return H::combine(std::move(h), e.op, e.type);
            }
        }

        operation op{};
        key type{};
        uint64_t value{};
    };

    friend bool operator==(const entity_value_diff&, const entity_value_diff&)
      = default;
    friend std::ostream& operator<<(std::ostream&, const entity_value_diff&);

    auto serde_fields() { return std::tie(entries); }

    absl::flat_hash_set<entry> entries;
};

struct alter_delta_cmd_data
  : serde::envelope<
      alter_delta_cmd_data,
      serde::version<1>,
      serde::compat_version<1>> {
    struct op
      : serde::envelope<op, serde::version<0>, serde::compat_version<0>> {
        client_quota::entity_key key;
        client_quota::entity_value_diff diff;
        auto serde_fields() { return std::tie(key, diff); }
    };

    std::vector<op> ops;

    auto serde_fields() { return std::tie(ops); }

    friend bool
    operator==(const alter_delta_cmd_data&, const alter_delta_cmd_data&)
      = default;
};

constexpr std::string_view to_string_view(entity_value_diff::key e) {
    /// Note: the string values of these enums need to match the values used in
    /// the kafka client quota handlers
    switch (e) {
    case entity_value_diff::key::producer_byte_rate:
        return "producer_byte_rate";
    case entity_value_diff::key::consumer_byte_rate:
        return "consumer_byte_rate";
    case entity_value_diff::key::controller_mutation_rate:
        return "controller_mutation_rate";
    }
}

template<typename E>
std::enable_if_t<std::is_enum_v<E>, std::optional<E>>
  from_string_view(std::string_view);

template<>
constexpr std::optional<entity_value_diff::key>
from_string_view<entity_value_diff::key>(std::string_view v) {
    return string_switch<std::optional<entity_value_diff::key>>(v)
      .match(
        to_string_view(entity_value_diff::key::producer_byte_rate),
        entity_value_diff::key::producer_byte_rate)
      .match(
        to_string_view(entity_value_diff::key::consumer_byte_rate),
        entity_value_diff::key::consumer_byte_rate)
      .match(
        to_string_view(entity_value_diff::key::controller_mutation_rate),
        entity_value_diff::key::controller_mutation_rate)
      .default_match(std::nullopt);
}

// Internal RPC request/response structs
struct alter_quotas_request
  : serde::envelope<
      alter_quotas_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    alter_delta_cmd_data cmd_data;
    model::timeout_clock::duration timeout{};

    auto serde_fields() { return std::tie(cmd_data, timeout); }

    friend bool
    operator==(const alter_quotas_request&, const alter_quotas_request&)
      = default;
};

struct alter_quotas_response
  : serde::envelope<
      alter_quotas_response,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    cluster::errc ec;
    auto serde_fields() { return std::tie(ec); }

    friend bool
    operator==(const alter_quotas_response&, const alter_quotas_response&)
      = default;
};

} // namespace cluster::client_quota
