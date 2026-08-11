/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/cluster_link/table_utils.h"

#include "cluster_link/model/types.h"
#include "ssx/async_algorithm.h"

namespace cluster::cluster_link {

using ::cluster_link::model::id_t;
using ::cluster_link::model::metadata;
using ::cluster_link::model::metadata_ptr;

ss::future<chunked_hash_map<id_t, metadata>>
copy_links_for_snapshot(chunked_hash_map<id_t, metadata_ptr> links) {
    chunked_hash_map<id_t, metadata> copy;
    copy.reserve(links.size());
    for (const auto& [id, md_ptr] : links) {
        copy.emplace(id, co_await md_ptr->copy());
    }
    co_return copy;
}
ss::future<chunked_hash_map<id_t, metadata_ptr>>
copy_links_from_snapshot(const chunked_hash_map<id_t, metadata>& links) {
    chunked_hash_map<id_t, metadata_ptr> copy;
    copy.reserve(links.size());
    for (const auto& [id, md] : links) {
        auto metadata_copy = ss::make_lw_shared<metadata>(co_await md.copy());

        copy.emplace(id, std::move(metadata_copy));
    }

    co_return copy;
}
} // namespace cluster::cluster_link
