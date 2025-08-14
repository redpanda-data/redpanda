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

#include <seastar/core/future.hh>

namespace experimental::cloud_topics::l1 {

class simple_metastore;
class simple_object_builder : public metastore::object_metadata_builder {
public:
    simple_object_builder()
      : object_metadata_builder() {}
    ~simple_object_builder() override {}
    simple_object_builder(const simple_object_builder&) = delete;
    simple_object_builder(simple_object_builder&&) = delete;
    simple_object_builder& operator=(const simple_object_builder&) = delete;
    simple_object_builder& operator=(simple_object_builder&&) = delete;

    object_id
    get_or_create_object_for(const model::topic_id_partition&) override;
    std::expected<void, error>
      add(object_id, metastore::object_metadata::ntp_metadata) override;
    std::expected<void, error> finish(object_id, size_t footer_pos) override;

    std::expected<chunked_vector<metastore::object_metadata>, error> release();

private:
    friend class simple_metastore;
    chunked_hash_map<object_id, metastore::object_metadata::ntp_metas_list_t>
      pending_objects_;
    chunked_vector<metastore::object_metadata> finished_objects_;
};

// Wrapper around state to implement the `metastore` interface.
// Not replicated or persisted, used for tests only.
class domain_manager;
class simple_metastore : public metastore {
public:
    std::unique_ptr<object_metadata_builder> object_builder() override;

    ss::future<std::expected<offsets_response, errc>>
    get_offsets(const model::topic_id_partition&) override;

    ss::future<std::expected<add_response, errc>>
      add_objects(std::unique_ptr<object_metadata_builder>) override;
    ss::future<std::expected<add_response, errc>>
    add_objects(const chunked_vector<object_metadata>&);

    ss::future<std::expected<void, errc>>
      replace_objects(std::unique_ptr<object_metadata_builder>) override;
    ss::future<std::expected<void, errc>>
    replace_objects(const chunked_vector<object_metadata>&);

    ss::future<std::expected<object_response, errc>>
    get_first_ge(const model::topic_id_partition&, kafka::offset) override;

    ss::future<std::expected<object_response, errc>>
    get_first_ge(const model::topic_id_partition&, model::timestamp) override;

    ss::future<std::expected<void, errc>> compact_objects(
      std::unique_ptr<object_metadata_builder>,
      const compaction_map_t&) override;
    ss::future<std::expected<void, errc>> compact_objects(
      const chunked_vector<object_metadata>&, const compaction_map_t&);

    ss::future<std::expected<compaction_offsets_response, errc>>
    get_compaction_offsets(
      const model::topic_id_partition&, model::timestamp) override;

private:
    friend class domain_manager;
    static std::expected<offsets_response, errc>
    get_offsets(const state&, const model::topic_id_partition&);
    static std::expected<object_response, errc>
    get_first_ge(const state&, const model::topic_id_partition&, kafka::offset);
    static std::expected<object_response, errc> get_first_ge(
      const state&, const model::topic_id_partition&, model::timestamp);
    static std::expected<compaction_offsets_response, errc>
    get_compaction_offsets(
      const state&, const model::topic_id_partition&, model::timestamp);

    state state_;
};

} // namespace experimental::cloud_topics::l1
