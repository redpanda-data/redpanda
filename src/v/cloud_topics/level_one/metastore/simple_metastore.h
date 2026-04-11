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
#include "cloud_topics/level_one/metastore/metastore.h"
#include "cloud_topics/level_one/metastore/state.h"
#include "model/timestamp.h"

#include <seastar/core/future.hh>

namespace cloud_topics::l1 {

class simple_metastore;
class simple_object_builder : public metastore::object_metadata_builder {
public:
    explicit simple_object_builder(state* s)
      : object_metadata_builder()
      , state_(s) {}
    ~simple_object_builder() override {}
    simple_object_builder(const simple_object_builder&) = delete;
    simple_object_builder(simple_object_builder&&) = delete;
    simple_object_builder& operator=(const simple_object_builder&) = delete;
    simple_object_builder& operator=(simple_object_builder&&) = delete;

    ss::future<std::expected<object_id, error>>
    get_or_create_object_for(const model::topic_id_partition&) override;
    ss::future<std::expected<object_id, error>>
    create_object_for(const model::topic_id_partition&) override;
    std::expected<void, error> remove_pending_object(object_id) override;
    std::expected<void, error>
      add(object_id, metastore::object_metadata::ntp_metadata) override;
    std::expected<void, error>
    finish(object_id, size_t footer_pos, size_t object_size) override;
    bool is_empty() const override;

    std::expected<chunked_vector<metastore::object_metadata>, error> release();

private:
    friend class simple_metastore;
    state* state_;
    chunked_hash_map<object_id, metastore::object_metadata::ntp_metas_list_t>
      pending_objects_;
    chunked_vector<metastore::object_metadata> finished_objects_;
};

// Wrapper around state to implement the `metastore` interface.
// Not replicated or persisted, used for tests only.
class simple_domain_manager;
class simple_metastore : public metastore {
public:
    ss::future<std::expected<std::unique_ptr<object_metadata_builder>, errc>>
    object_builder() override;

    ss::future<std::expected<offsets_response, errc>>
    get_offsets(const model::topic_id_partition&) override;

    ss::future<std::expected<size_response, errc>>
    get_size(const model::topic_id_partition&) override;

    ss::future<std::expected<add_response, errc>> add_objects(
      const object_metadata_builder&, const term_offset_map_t&) override;
    ss::future<std::expected<add_response, errc>> add_objects(
      const chunked_vector<object_metadata>&, const term_offset_map_t&);

    ss::future<std::expected<void, errc>>
    replace_objects(const object_metadata_builder&) override;
    ss::future<std::expected<void, errc>>
    replace_objects(const chunked_vector<object_metadata>&);

    ss::future<std::expected<void, errc>>
    set_start_offset(const model::topic_id_partition&, kafka::offset) override;

    ss::future<std::expected<topic_removal_response, errc>>
    remove_topics(const chunked_vector<model::topic_id>&) override;

    ss::future<std::expected<object_response, errc>>
    get_first_ge(const model::topic_id_partition&, kafka::offset) override;

    ss::future<std::expected<object_response, errc>> get_first_ge(
      const model::topic_id_partition&,
      kafka::offset,
      model::timestamp) override;

    ss::future<std::expected<kafka::offset, errc>> get_first_offset_for_bytes(
      const model::topic_id_partition&, uint64_t size) override;

    ss::future<std::expected<kafka::offset, errc>> get_end_offset_for_term(
      const model::topic_id_partition&, model::term_id) override;

    ss::future<std::expected<model::term_id, errc>> get_term_for_offset(
      const model::topic_id_partition&, kafka::offset) override;

    ss::future<std::expected<void, errc>> compact_objects(
      const object_metadata_builder&, const compaction_map_t&) override;
    ss::future<std::expected<void, errc>> compact_objects(
      const chunked_vector<object_metadata>&, const compaction_map_t&);

    void preregister_objects(const chunked_vector<object_id>&);

    ss::future<std::expected<compaction_offsets_response, errc>>
    get_compaction_offsets(const model::topic_id_partition&, model::timestamp);

    ss::future<std::expected<compaction_info_response, errc>>
    get_compaction_info(const compaction_info_spec&) override;

    ss::future<std::expected<compaction_info_map, errc>>
    get_compaction_infos(const chunked_vector<compaction_info_spec>&) override;

    ss::future<std::expected<extent_metadata_response, errc>>
    get_extent_metadata_forwards(
      const model::topic_id_partition&,
      kafka::offset,
      kafka::offset,
      size_t,
      include_object_metadata) override;

    ss::future<std::expected<extent_metadata_response, errc>>
    get_extent_metadata_backwards(
      const model::topic_id_partition&,
      kafka::offset,
      kafka::offset,
      size_t) override;

    ss::future<std::expected<std::nullopt_t, errc>> flush() override {
        co_return std::unexpected(errc::transport_error);
    }

    ss::future<std::expected<std::nullopt_t, errc>>
    restore(const cloud_storage::remote_label&) override {
        co_return std::unexpected(errc::transport_error);
    }

private:
    friend class simple_domain_manager;
    static std::expected<offsets_response, errc>
    get_offsets(const state&, const model::topic_id_partition&);
    static std::expected<size_response, errc>
    get_size(const state&, const model::topic_id_partition&);
    static std::expected<object_response, errc>
    get_first_ge(const state&, const model::topic_id_partition&, kafka::offset);
    static std::expected<object_response, errc> get_first_ge(
      const state&,
      const model::topic_id_partition&,
      kafka::offset,
      model::timestamp);
    static std::expected<kafka::offset, errc> get_first_offset_for_bytes(
      const state&, const model::topic_id_partition&, uint64_t size);
    static std::expected<compaction_offsets_response, errc>
    get_compaction_offsets(
      const state&, const model::topic_id_partition&, model::timestamp);
    static std::expected<double, errc>
    get_dirty_ratio(const state&, const model::topic_id_partition&);
    static std::expected<std::optional<model::timestamp>, errc>
    get_earliest_dirty_ts(const state&, const model::topic_id_partition&);
    static std::expected<compaction_epoch, errc>
    get_compaction_epoch(const state&, const model::topic_id_partition&);
    static std::expected<compaction_info_response, errc> get_compaction_info(
      const state&, const model::topic_id_partition&, model::timestamp);
    static std::expected<kafka::offset, errc> get_end_offset_for_term(
      const state&, const model::topic_id_partition&, model::term_id);
    static std::expected<model::term_id, errc> get_term_for_offset(
      const state&, const model::topic_id_partition&, kafka::offset);
    static std::expected<extent_metadata_response, errc>
    get_extent_metadata_forwards(
      const state&,
      const model::topic_id_partition&,
      kafka::offset,
      kafka::offset,
      size_t,
      include_object_metadata);
    static std::expected<extent_metadata_response, errc>
    get_extent_metadata_backwards(
      const state&,
      const model::topic_id_partition&,
      kafka::offset,
      kafka::offset,
      size_t);

    state state_;
};

} // namespace cloud_topics::l1
