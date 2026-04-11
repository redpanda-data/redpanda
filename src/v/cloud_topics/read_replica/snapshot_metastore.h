/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "cloud_topics/level_one/metastore/lsm/state_reader.h"
#include "cloud_topics/level_one/metastore/metastore.h"
#include "lsm/lsm.h"

#include <seastar/core/gate.hh>

namespace cloud_topics::read_replica {

/// \brief Implements the metastore interface backed by a single LSM snapshot.
///
/// This is meant to be used in the context of a single partition. As such, it
/// operates only on a single domain of the metastore.
///
/// Some read-oriented interface methods (extent metadata, size-based offset
/// lookup, compaction info) are left unimplemented because they only serve
/// write-side processes such as retention and compaction. All write methods
/// (add_objects, replace_objects, set_start_offset, compact_objects, etc.) are
/// also unimplemented since this is a read-only snapshot.
class snapshot_metastore : public l1::metastore {
public:
    explicit snapshot_metastore(ss::gate::holder, lsm::snapshot snapshot);

    ss::future<std::expected<object_response, errc>>
    get_first_ge(const model::topic_id_partition&, kafka::offset) override;

    ss::future<std::expected<std::unique_ptr<object_metadata_builder>, errc>>
    object_builder() override;

    ss::future<std::expected<offsets_response, errc>>
    get_offsets(const model::topic_id_partition&) override;

    ss::future<std::expected<size_response, errc>>
    get_size(const model::topic_id_partition&) override;

    ss::future<std::expected<add_response, errc>> add_objects(
      const object_metadata_builder&, const term_offset_map_t&) override;

    ss::future<std::expected<void, errc>>
    replace_objects(const object_metadata_builder&) override;

    ss::future<std::expected<void, errc>>
    set_start_offset(const model::topic_id_partition&, kafka::offset) override;

    ss::future<std::expected<topic_removal_response, errc>>
    remove_topics(const chunked_vector<model::topic_id>&) override;

    // Timestamp-based query - implemented for timequery support.
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

    ss::future<std::expected<std::nullopt_t, errc>> flush() override;

    ss::future<std::expected<std::nullopt_t, errc>>
    restore(const cloud_storage::remote_label&) override;

private:
    // Gate holder that ensures we don't destruct the owner of the underlying
    // LSM snapshot.
    ss::gate::holder gate_holder_;

    // Reader that owns and operates on a given LSM snapshot.
    l1::state_reader reader_;
};

} // namespace cloud_topics::read_replica
