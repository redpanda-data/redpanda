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

// Wrapper around state to implement the `metastore` interface.
// Not replicated or persisted, used for tests only.
class simple_metastore : public metastore {
public:
    static constexpr auto simple_domain_id = metastore::domain_id{0};
    ss::future<std::expected<metastore::domain_id, errc>>
    get_domain_id(const model::topic_id_partition&) const override {
        // In the name of simplicity, make all data belong to the same domain.
        co_return simple_domain_id;
    }

    ss::future<std::expected<offsets_response, errc>>
    get_offsets(const model::topic_id_partition&) override;

    ss::future<std::expected<void, errc>>
    add_objects(domain_id, const chunked_vector<object_metadata>&) override;

    ss::future<std::expected<void, errc>>
    replace_objects(domain_id, const chunked_vector<object_metadata>&) override;

    ss::future<std::expected<object_response, errc>>
    get_first_ge(const model::topic_id_partition&, kafka::offset) override;

    ss::future<std::expected<object_response, errc>>
    get_first_ge(const model::topic_id_partition&, model::timestamp) override;

    ss::future<std::expected<void, errc>> compact_objects(
      const chunked_vector<object_metadata>&, const compaction_map_t&) override;

    ss::future<std::expected<compaction_offsets_response, errc>>
    get_compaction_offsets(
      const model::topic_id_partition&, model::timestamp) override;

private:
    state state_;
};

} // namespace experimental::cloud_topics::l1
