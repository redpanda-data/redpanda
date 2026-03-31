/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/frontend_reader/l1_footer_cache.h"

namespace cloud_topics {

l1_footer_cache::l1_footer_cache(size_t max_entries) {
    if (max_entries > 0) {
        _cache = std::make_unique<kv_cache_t>(kv_cache_t::config{
          .cache_size = max_entries,
          .small_size = max_entries / 10,
        });
    }
}

ss::shared_ptr<const l1::footer> l1_footer_cache::get(l1::object_id oid) {
    if (!_cache) {
        return nullptr;
    }
    auto result = _cache->get_value(oid);
    if (!result) {
        return nullptr;
    }
    return *result;
}

void l1_footer_cache::put(
  l1::object_id oid, ss::shared_ptr<const l1::footer> footer) {
    if (_cache) {
        _cache->try_insert(oid, std::move(footer));
    }
}

ss::future<> l1_footer_cache::stop() { return ss::make_ready_future<>(); }

} // namespace cloud_topics
