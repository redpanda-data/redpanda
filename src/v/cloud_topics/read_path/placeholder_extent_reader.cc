/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/read_path/placeholder_extent_reader.h"

#include "cloud_io/basic_cache_service_api.h"
#include "cloud_io/io_result.h"
#include "cloud_io/remote.h"
#include "cloud_topics/dl_placeholder.h"
#include "cloud_topics/errc.h"
#include "cloud_topics/logger.h"
#include "cloud_topics/read_path/placeholder_extent.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "model/record_batch_types.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/coroutine/as_future.hh>

using namespace std::chrono_literals;

namespace experimental::cloud_topics {

/// Convert record batch to dl_placeholder value and return an empty
/// extent which later has to be hydrated.
size_t get_extent_size(const model::record_batch& placeholder) {
    /// FIXME: This code makes unnecessary copy
    vassert(
      placeholder.header().type == model::record_batch_type::dl_placeholder,
      "Unsupported batch type {}",
      placeholder.header());
    iobuf payload = placeholder.data().copy();
    iobuf_parser parser(std::move(payload));
    auto record = model::parse_one_record_from_buffer(parser);
    iobuf value = std::move(record).release_value();
    auto p = serde::from_iobuf<dl_placeholder>(std::move(value));
    return p.size_bytes;
}

ss::future<result<ss::circular_buffer<placeholder_extent>>>
materialize_sorted_run(
  ss::circular_buffer<model::record_batch> placeholders,
  cloud_storage_clients::bucket_name bucket,
  cloud_io::remote_api<>* api,
  cloud_io::basic_cache_service_api<>* cache,
  retry_chain_node* rtc) {
    absl::node_hash_map<uuid_t, ss::lw_shared_ptr<hydrated_L0_object>> hydrated;
    ss::circular_buffer<placeholder_extent> extents;
    for (auto&& p : placeholders) {
        auto extent = make_placeholder_extent(std::move(p));

        extents.push_back(extent);
        // reuse hydrated objects if possible
        auto it = hydrated.find(extent.placeholder.id());
        if (it != hydrated.end()) {
            auto& payload = it->second->payload;
            // TODO: check that id of the payload matches
            extent.L0_object->payload = payload.share(0, payload.size_bytes());
        } else {
            auto res = co_await materialize(&extent, bucket, api, cache, rtc);
            if (res.has_error()) {
                co_return res.error();
            }
            hydrated.insert(
              std::make_pair(extent.placeholder.id, extent.L0_object));
        }
    }
    co_return std::move(extents);
}

ss::future<ss::circular_buffer<model::record_batch>> materialize_placeholders(
  cloud_storage_clients::bucket_name bucket,
  ss::circular_buffer<model::record_batch> underlying,
  cloud_io::remote_api<ss::lowres_clock>& api,
  cloud_io::basic_cache_service_api<ss::lowres_clock>& cache,
  retry_chain_node& rtc,
  retry_chain_logger& logger) {
    auto extents = co_await materialize_sorted_run(
      std::move(underlying), bucket, &api, &cache, &rtc);
    if (extents.has_error()) {
        vlog(
          logger.error,
          "Failed to materialize sorted run: {}",
          extents.error().message());
        throw std::system_error(extents.error());
    }

    ss::circular_buffer<model::record_batch> results;
    for (auto& e : extents.value()) {
        results.push_back(make_raft_data_batch(std::move(e)));
    }

    co_return std::move(results);
}

} // namespace experimental::cloud_topics
