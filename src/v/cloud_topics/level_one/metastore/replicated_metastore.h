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
#include "cloud_io/remote.h"
#include "cloud_storage_clients/types.h"
#include "cloud_topics/level_one/metastore/manifest_io.h"
#include "cloud_topics/level_one/metastore/metastore.h"
#include "model/fundamental.h"
#include "storage/fwd.h"

#include <memory>

namespace cloud_topics::l1 {
class leader_router;

// Implementation of the `metastore` interface that routes metadata requests
// with the provided frontend (e.g. sending to the leader of a given metastore
// topic partition).
class replicated_metastore : public metastore {
public:
    replicated_metastore(
      leader_router& fe,
      cloud_io::remote& io,
      cloud_storage_clients::bucket_name bucket);

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

    ss::future<std::expected<std::nullopt_t, errc>> flush() override;

    ss::future<std::expected<std::nullopt_t, errc>>
    restore(const cloud_storage::remote_label&) override;

private:
    leader_router& fe_;
    std::unique_ptr<manifest_io> manifest_io_;
};

} // namespace cloud_topics::l1
