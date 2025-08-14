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

namespace experimental::cloud_topics::l1 {
class frontend;

// Implementation of the `metastore` interface that routes metadata requests
// with the provided frontend (e.g. sending to the leader of a given metastore
// topic partition).
class replicated_metastore : public metastore {
public:
    explicit replicated_metastore(frontend& fe);

    std::unique_ptr<object_metadata_builder> object_builder() override;

    ss::future<std::expected<offsets_response, errc>>
    get_offsets(const model::topic_id_partition&) override;

    ss::future<std::expected<add_response, errc>>
      add_objects(std::unique_ptr<object_metadata_builder>) override;

    ss::future<std::expected<void, errc>>
      replace_objects(std::unique_ptr<object_metadata_builder>) override;

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
    frontend& fe_;
};

} // namespace experimental::cloud_topics::l1
