/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/seastarx.h"
#include "cloud_topics/level_one/metastore/domain_uuid.h"
#include "cloud_topics/level_one/metastore/lsm/state.h"
#include "cloud_topics/level_one/metastore/lsm/write_batch_row.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "serde/envelope.h"
#include "utils/named_type.h"

#include <expected>

namespace cloud_topics::l1 {

enum class lsm_update_key : uint8_t {
    apply_write_batch = 0,
    persist_manifest = 1,
    set_domain_uuid = 2,
    reset_manifest = 3,
};

using lsm_update_error = named_type<ss::sstring, struct lsm_update_error_tag>;

// Adds the given rows to the volatile buffer, assigning each row sequence
// numbers upon applying.
struct apply_write_batch_update
  : public serde::envelope<
      apply_write_batch_update,
      serde::version<0>,
      serde::compat_version<0>> {
    friend bool
    operator==(const apply_write_batch_update&, const apply_write_batch_update&)
      = default;
    auto serde_fields() { return std::tie(expected_uuid, rows); }

    static std::expected<apply_write_batch_update, lsm_update_error> build(
      const lsm_state&,
      domain_uuid expected_uuid,
      chunked_vector<write_batch_row> rows);
    std::expected<std::monostate, lsm_update_error>
    can_apply(const lsm_state&) const;
    std::expected<std::monostate, lsm_update_error>
    apply(lsm_state&, model::offset);

    apply_write_batch_update share();

    domain_uuid expected_uuid;
    chunked_vector<write_batch_row> rows;
};

// Sets the persisted_manifest, with the expectation that the manifest has been
// persisted to object storage.
struct persist_manifest_update
  : public serde::envelope<
      persist_manifest_update,
      serde::version<0>,
      serde::compat_version<0>> {
    auto serde_fields() { return std::tie(expected_uuid, manifest); }
    friend bool operator==(
      const persist_manifest_update&, const persist_manifest_update&) = default;

    static std::expected<persist_manifest_update, lsm_update_error> build(
      const lsm_state&, domain_uuid, lsm_state::serialized_manifest manifest);
    std::expected<std::monostate, lsm_update_error>
    can_apply(const lsm_state&) const;
    std::expected<std::monostate, lsm_update_error> apply(lsm_state&);

    domain_uuid expected_uuid;
    lsm_state::serialized_manifest manifest;
};

// Sets the domain UUID. This should be the first operation performed to the
// STM, to ensure all replicas are operating on the same paths in object
// storage.
struct set_domain_uuid_update
  : public serde::envelope<
      set_domain_uuid_update,
      serde::version<0>,
      serde::compat_version<0>> {
    friend bool operator==(
      const set_domain_uuid_update&, const set_domain_uuid_update&) = default;
    auto serde_fields() { return std::tie(uuid); }

    static std::expected<set_domain_uuid_update, lsm_update_error>
    build(const lsm_state&, domain_uuid);
    std::expected<std::monostate, lsm_update_error>
    can_apply(const lsm_state&) const;
    std::expected<std::monostate, lsm_update_error> apply(lsm_state&);

    domain_uuid uuid;
};

// Resets the manifest to match the contents of the given serialized manifest
// (expected to be a manifest stored in shared object storage), and updates the
// domain UUID accordingly. This is meant to be used as the basis for recovery.
struct reset_manifest_update
  : public serde::envelope<
      reset_manifest_update,
      serde::version<0>,
      serde::compat_version<0>> {
    auto serde_fields() { return std::tie(new_uuid, new_manifest); }
    friend bool operator==(
      const reset_manifest_update&, const reset_manifest_update&) = default;

    static std::expected<reset_manifest_update, lsm_update_error> build(
      const lsm_state&,
      domain_uuid,
      std::optional<lsm_state::serialized_manifest>);
    std::expected<std::monostate, lsm_update_error>
    can_apply(const lsm_state&) const;
    std::expected<std::monostate, lsm_update_error>
    apply(lsm_state&, model::term_id, model::offset);

    domain_uuid new_uuid;
    std::optional<lsm_state::serialized_manifest> new_manifest;
};
} // namespace cloud_topics::l1

template<>
struct fmt::formatter<cloud_topics::l1::lsm_update_key> final
  : fmt::formatter<std::string_view> {
    template<typename FormatContext>
    auto format(
      const cloud_topics::l1::lsm_update_key& k, FormatContext& ctx) const {
        switch (k) {
        case cloud_topics::l1::lsm_update_key::apply_write_batch:
            return formatter<string_view>::format("apply_write_batch", ctx);
        case cloud_topics::l1::lsm_update_key::persist_manifest:
            return formatter<string_view>::format("persist_manifest", ctx);
        case cloud_topics::l1::lsm_update_key::set_domain_uuid:
            return formatter<string_view>::format("set_domain_uuid", ctx);
        case cloud_topics::l1::lsm_update_key::reset_manifest:
            return formatter<string_view>::format("reset_manifest", ctx);
        }
    }
};
