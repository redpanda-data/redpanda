// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/client_quota_serde.h"
#include "serde/rw/envelope.h"
#include "serde/rw/rw.h"
#include "serde/rw/set.h"     // IWYU pragma: keep
#include "serde/rw/sstring.h" // IWYU pragma: keep

#include <seastar/util/variant_utils.hh>

#include <fmt/ranges.h> // IWYU pragma: keep
#include <gtest/gtest.h>

#include <stdexcept>

namespace cluster::client_quota {

namespace {

// serialize and deserialize to specified type
template<typename To, typename From>
To serde_to(const From& from) {
    auto b = serde::to_iobuf(from);
    return serde::from_iobuf<To>(std::move(b));
}
} // namespace

using part_variant_v0 = entity_key::part_v0::variant;
using part_variant = entity_key::part::variant;

struct from_v0 {
    // All types of v0 variant have an equivalent in part_variant
    template<typename Match>
    part_variant operator()(const Match& m) const {
        return {m};
    }
};

struct legacy_entity_key
  : serde::
      envelope<legacy_entity_key, serde::version<0>, serde::compat_version<0>> {
public:
    template<typename... T>
    explicit legacy_entity_key(T&&... t)
      : parts{{.part = std::forward<T>(t)}...} {}

    auto serde_fields() { return std::tie(parts); }

    friend bool
    operator==(const legacy_entity_key&, const legacy_entity_key&) = default;
    friend std::ostream& operator<<(std::ostream&, const legacy_entity_key&);

    template<typename H>
    friend H AbslHashValue(H h, const legacy_entity_key& e) {
        return AbslHashValue(std::move(h), e.parts);
    }

    absl::flat_hash_set<entity_key::part_v0> parts;

    entity_key to_entity_key() const {
        entity_key ret;
        for (const auto& p : parts) {
            ret.parts.insert(entity_key::part_t{ss::visit(p.part, from_v0{})});
        }
        return ret;
    }
};

std::ostream& operator<<(std::ostream& os, const legacy_entity_key& key) {
    fmt::print(os, "{{parts: {}}}", key.parts);
    return os;
}

TEST(client_quota_serde, round_trip) {
    const std::vector<part_variant> all_values{
      entity_key::part::client_id_match{.value = "my-consumer"},
      entity_key::part::client_id_prefix_match{.value = "my-cons"},
      entity_key::part::client_id_default_match{},
      entity_key::part::user_match{.value = "alice"},
      entity_key::part::user_default_match{}};

    // Ensure that the custom serde read/write works as expected for inner type
    for (const auto& value : all_values) {
        entity_key::part part{.part = value};
        const auto rt_part = serde_to<entity_key::part>(part);
        EXPECT_EQ(part, rt_part);
    }

    // Ensure that the custom serde read/write works as expected for outer type
    for (const auto& value : all_values) {
        entity_key key{value};
        const auto rt_key = serde_to<entity_key>(key);
        EXPECT_EQ(key, rt_key);
    }
}

TEST(client_quota_serde, read_legacy_value_v0) {
    const std::vector<part_variant_v0> v0_values{
      entity_key::part::client_id_match{.value = "my-consumer"},
      entity_key::part::client_id_prefix_match{.value = "my-cons"},
      entity_key::part::client_id_default_match{}};

    for (const auto& v0_value : v0_values) {
        const auto expected_part = ss::visit(v0_value, from_v0{});

        entity_key::part_v0 legacy_part{.part = v0_value};
        const auto part = serde_to<entity_key::part>(legacy_part);
        EXPECT_EQ(part.part, expected_part);
    }
}

TEST(client_quota_serde, part_compatibility) {
    const std::vector<part_variant> v0_compatible{
      entity_key::part::client_id_match{.value = "my-consumer"},
      entity_key::part::client_id_prefix_match{.value = "my-cons"},
      entity_key::part::client_id_default_match{}};

    for (const auto& v : v0_compatible) {
        entity_key::part p{.part = v};
        EXPECT_TRUE(p.is_v0_compat()) << ss::format("Match: {}\n", p);
    }

    const std::vector<part_variant> v0_incompatible{
      entity_key::part::user_match{.value = "alice"},
      entity_key::part::user_default_match{}};

    for (const auto& v : v0_incompatible) {
        entity_key::part p{.part = v};
        EXPECT_FALSE(p.is_v0_compat()) << ss::format("Match: {}\n", p);
    }
}

TEST(client_quota_serde, entity_key_compatibility) {
    const std::vector<part_variant> v0_compatible{
      entity_key::part::client_id_match{.value = "my-consumer"},
      entity_key::part::client_id_prefix_match{.value = "my-cons"},
      entity_key::part::client_id_default_match{}};

    // backwards compatible
    for (const auto& v : v0_compatible) {
        entity_key key{v};
        const auto rt_key = serde_to<legacy_entity_key>(key).to_entity_key();
        EXPECT_EQ(key, rt_key);
    }

    const auto v0_fields = [](const auto& v) -> part_variant_v0 { return {v}; };
    const auto not_v0_fields = [](const auto& v) -> part_variant_v0 {
        throw std::runtime_error{
          ss::format("'{}' not supported by part_variant_v0", v)};
    };

    // forwards compatible
    for (const auto& v : v0_compatible) {
        auto v0 = ss::visit(
          v, entity_key::part_v0::visitor{v0_fields, not_v0_fields});
        legacy_entity_key key{v0};
        const auto rt_key = serde_to<entity_key>(key);
        EXPECT_EQ(key.to_entity_key(), rt_key);
    }
}

} // namespace cluster::client_quota
