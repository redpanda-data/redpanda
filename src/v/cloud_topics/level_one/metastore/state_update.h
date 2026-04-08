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

#include "absl/container/btree_set.h"
#include "base/seastarx.h"
#include "cloud_topics/level_one/metastore/state.h"
#include "cloud_topics/level_one/metastore/state_update_utils.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"

#include <expected>

namespace cloud_topics::l1 {

enum class update_key : uint8_t {
    add_objects = 0,
    replace_objects = 1,
    set_start_offset = 2,
    remove_objects = 3,
    remove_topics = 4,
    preregister_objects = 5,
    expire_preregistered_objects = 6,
};

using stm_update_error = named_type<ss::sstring, struct update_error_tag>;

struct new_object
  : public serde::
      envelope<new_object, serde::version<0>, serde::compat_version<0>> {
    struct metadata
      : public serde::
          envelope<metadata, serde::version<0>, serde::compat_version<0>> {
        friend bool operator==(const metadata&, const metadata&) = default;
        auto serde_fields() {
            return std::tie(
              base_offset, last_offset, max_timestamp, filepos, len);
        }

        kafka::offset base_offset;
        kafka::offset last_offset;
        model::timestamp max_timestamp;
        size_t filepos;
        size_t len;
    };

    friend bool operator==(const new_object&, const new_object&) = default;
    auto serde_fields() {
        return std::tie(oid, footer_pos, object_size, extent_metas);
    }

    object_id oid;
    size_t footer_pos;
    size_t object_size;
    chunked_hash_map<
      model::topic_id,
      chunked_hash_map<model::partition_id, metadata>>
      extent_metas;

    // Returns the sum of lengths of the extents collected.
    size_t collect_extents_by_tidp(sorted_extents_by_tidp_t*) const;
};

using term_state_update_t
  = chunked_hash_map<model::topic_id_partition, chunked_vector<term_start>>;

struct add_objects_update
  : public serde::envelope<
      add_objects_update,
      serde::version<0>,
      serde::compat_version<0>> {
    friend bool
    operator==(const add_objects_update&, const add_objects_update&) = default;
    auto serde_fields() { return std::tie(new_objects, new_terms); }

    static constexpr auto key{update_key::add_objects};
    static std::expected<add_objects_update, stm_update_error> build(
      const state&,
      chunked_vector<new_object>,
      term_state_update_t,
      chunked_hash_map<model::topic_id_partition, kafka::offset>* = nullptr);

    std::expected<std::monostate, stm_update_error> can_apply(
      const state&,
      chunked_hash_map<model::topic_id_partition, kafka::offset>* = nullptr);
    std::expected<std::monostate, stm_update_error> apply(state&);

    chunked_vector<new_object> new_objects;
    term_state_update_t new_terms;
};

struct compaction_state_update
  : public serde::envelope<
      compaction_state_update,
      serde::version<2>,
      serde::compat_version<0>> {
    // NOTE: intentionally duplicate code from
    // metastore::compaction_update::cleaned_range, defined separately to
    // decouple the public interface (metastore) from the underlying stored
    // state.
    struct cleaned_range
      : public serde::
          envelope<cleaned_range, serde::version<0>, serde::compat_version<0>> {
        friend bool
        operator==(const cleaned_range&, const cleaned_range&) = default;
        auto serde_fields() {
            return std::tie(base_offset, last_offset, has_tombstones);
        }

        kafka::offset base_offset;
        kafka::offset last_offset;

        // Whether the cleaned range has tombstones.
        bool has_tombstones{false};
    };
    auto serde_fields() {
        return std::tie(
          new_cleaned_ranges,
          removed_tombstones_ranges,
          cleaned_at,
          expected_compaction_epoch);
    }
    // The cleaned ranges for this compaction, if any. Ranges may or may not
    // have tombstones.
    chunked_vector<cleaned_range> new_cleaned_ranges;

    // Expected that these ranges correspond to existing cleaned ranges with
    // tombstones, and indicate that these ranges may be removed.
    offset_interval_set removed_tombstones_ranges;

    // Timestamp at which this compaction operation was run.
    model::timestamp cleaned_at;

    // The expected compaction epoch.
    partition_state::compaction_epoch_t expected_compaction_epoch;
};

struct replace_objects_update
  : public serde::envelope<
      replace_objects_update,
      serde::version<0>,
      serde::compat_version<0>> {
    friend bool operator==(
      const replace_objects_update&, const replace_objects_update&) = default;
    auto serde_fields() { return std::tie(new_objects, compaction_updates); }

    static constexpr auto key{update_key::replace_objects};
    static std::expected<replace_objects_update, stm_update_error> build(
      const state&,
      chunked_vector<new_object>,
      chunked_hash_map<model::topic_id_partition, compaction_state_update>
      = {});

    std::expected<std::monostate, stm_update_error> can_apply(const state&);
    std::expected<std::monostate, stm_update_error> apply(state&);

    chunked_vector<new_object> new_objects;

    // The new cleaned ranges that are represented in 'new_objects', if any.
    chunked_hash_map<
      model::topic_id,
      chunked_hash_map<model::partition_id, compaction_state_update>>
      compaction_updates;
};

struct set_start_offset_update
  : public serde::envelope<
      set_start_offset_update,
      serde::version<0>,
      serde::compat_version<0>> {
    friend bool operator==(
      const set_start_offset_update&, const set_start_offset_update&) = default;
    auto serde_fields() { return std::tie(tp, new_start_offset); }

    static constexpr auto key{update_key::set_start_offset};
    static std::expected<set_start_offset_update, stm_update_error> build(
      const state&,
      const model::topic_id_partition&,
      kafka::offset,
      bool* is_no_op = nullptr);

    std::expected<std::monostate, stm_update_error>
    can_apply(const state&, bool* is_no_op = nullptr);
    std::expected<std::monostate, stm_update_error> apply(state&);

    model::topic_id_partition tp;
    kafka::offset new_start_offset;
};

struct remove_objects_update
  : public serde::envelope<
      remove_objects_update,
      serde::version<0>,
      serde::compat_version<0>> {
    friend bool operator==(
      const remove_objects_update&, const remove_objects_update&) = default;
    auto serde_fields() { return std::tie(objects); }
    static constexpr auto key{update_key::remove_objects};

    static std::expected<remove_objects_update, stm_update_error>
    build(const state&, chunked_vector<object_id>);

    std::expected<std::monostate, stm_update_error> can_apply(const state&);
    std::expected<std::monostate, stm_update_error> apply(state&);

    // The set of objects to remove. Can be applied only if all the objects in
    // the list no longer have any extents that reference it, as indicated by
    // the object's removed_data_size.
    chunked_vector<object_id> objects;
};

struct remove_topics_update
  : public serde::envelope<
      remove_topics_update,
      serde::version<0>,
      serde::compat_version<0>> {
    friend bool operator==(
      const remove_topics_update&, const remove_topics_update&) = default;
    auto serde_fields() { return std::tie(topics); }
    static constexpr auto key{update_key::remove_topics};

    static std::expected<remove_topics_update, stm_update_error>
    build(const state&, chunked_vector<model::topic_id>);

    std::expected<std::monostate, stm_update_error> can_apply(const state&);
    std::expected<std::monostate, stm_update_error> apply(state&);

    chunked_vector<model::topic_id> topics;
};

struct preregister_objects_update
  : public serde::envelope<
      preregister_objects_update,
      serde::version<0>,
      serde::compat_version<0>> {
    friend bool operator==(
      const preregister_objects_update&,
      const preregister_objects_update&) = default;
    auto serde_fields() { return std::tie(object_ids, registered_at); }

    static constexpr auto key{update_key::preregister_objects};

    std::expected<std::monostate, stm_update_error> can_apply(const state&);
    std::expected<std::monostate, stm_update_error> apply(state&);

    chunked_vector<object_id> object_ids;
    model::timestamp registered_at;
};

struct expire_preregistered_objects_update
  : public serde::envelope<
      expire_preregistered_objects_update,
      serde::version<0>,
      serde::compat_version<0>> {
    friend bool operator==(
      const expire_preregistered_objects_update&,
      const expire_preregistered_objects_update&) = default;
    auto serde_fields() { return std::tie(object_ids); }

    static constexpr auto key{update_key::expire_preregistered_objects};

    std::expected<std::monostate, stm_update_error> can_apply(const state&);
    std::expected<std::monostate, stm_update_error> apply(state&);

    chunked_vector<object_id> object_ids;
};

} // namespace cloud_topics::l1

template<>
struct fmt::formatter<cloud_topics::l1::update_key> final
  : fmt::formatter<std::string_view> {
    template<typename FormatContext>
    auto
    format(const cloud_topics::l1::update_key& k, FormatContext& ctx) const {
        switch (k) {
        case cloud_topics::l1::update_key::add_objects:
            return formatter<string_view>::format("add_objects", ctx);
        case cloud_topics::l1::update_key::replace_objects:
            return formatter<string_view>::format("replace_objects", ctx);
        case cloud_topics::l1::update_key::set_start_offset:
            return formatter<string_view>::format("set_start_offset", ctx);
        case cloud_topics::l1::update_key::remove_objects:
            return formatter<string_view>::format("remove_objects", ctx);
        case cloud_topics::l1::update_key::remove_topics:
            return formatter<string_view>::format("remove_topics", ctx);
        case cloud_topics::l1::update_key::preregister_objects:
            return formatter<string_view>::format("preregister_objects", ctx);
        case cloud_topics::l1::update_key::expire_preregistered_objects:
            return formatter<string_view>::format(
              "expire_preregistered_objects", ctx);
        }
    }
};
