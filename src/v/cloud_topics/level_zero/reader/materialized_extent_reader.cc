/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/reader/materialized_extent_reader.h"

#include "cloud_io/basic_cache_service_api.h"
#include "cloud_io/io_result.h"
#include "cloud_io/remote.h"
#include "cloud_topics/errc.h"
#include "cloud_topics/level_zero/reader/materialized_extent.h"
#include "cloud_topics/logger.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/coroutine/as_future.hh>

using namespace std::chrono_literals;

namespace cloud_topics::l0 {

namespace {

ss::future<result<chunked_vector<materialized_extent>>> materialize_sorted_run(
  chunked_vector<extent_meta> query,
  cloud_storage_clients::bucket_name bucket,
  cloud_io::remote_api<>* api,
  cloud_io::basic_cache_service_api<>* cache,
  allow_materialization_failure allow_mat_failure,
  retry_chain_node* rtc,
  micro_probe* probe) {
    absl::node_hash_map<object_id, iobuf> hydrated;
    chunked_vector<materialized_extent> extents;
    for (const auto& extent : query) {
        extents.push_back(materialized_extent{.meta = extent});
        auto& back = extents.back();
        // reuse hydrated objects if possible
        auto it = hydrated.find(back.meta.id);
        if (it != hydrated.end()) {
            auto& payload = it->second;
            // TODO: check that id of the payload matches
            back.object = payload.share(0, payload.size_bytes());
        } else {
            auto res = co_await materialize(
              &back, bucket, api, cache, rtc, probe);
            if (!res.has_value()) {
                if (
                  bool(allow_mat_failure)
                  && res.error() == errc::download_not_found) {
                    vlog(
                      cd_log.warn,
                      "Skipping extent {} (offsets {}~{}) due to missing "
                      "object",
                      back.meta.id,
                      back.meta.base_offset,
                      back.meta.last_offset);
                    extents.pop_back();
                    continue;
                }
                co_return res.error();
            }
            // If reading from cache (res.value() == true), only the
            // required range was returned. Otherwise, the object
            // was hydrated and we can place it into the `hydrated`
            // collection.
            if (!res.value()) {
                hydrated.insert(
                  std::make_pair(
                    back.meta.id,
                    back.object.share(0, back.object.size_bytes())));
            }
        }
    }
    co_return std::move(extents);
}

} // namespace

ss::future<materialize_result> materialize_placeholders(
  cloud_storage_clients::bucket_name bucket,
  chunked_vector<extent_meta> query,
  cloud_io::remote_api<ss::lowres_clock>& api,
  cloud_io::basic_cache_service_api<ss::lowres_clock>& cache,
  allow_materialization_failure allow_mat_failure,
  retry_chain_node& rtc,
  retry_chain_logger& logger) {
    micro_probe probe;
    auto extents = co_await materialize_sorted_run(
      std::move(query), bucket, &api, &cache, allow_mat_failure, &rtc, &probe);
    if (!extents.has_value()) {
        vlog(
          logger.warn,
          "Failed to materialize sorted run: {}",
          extents.error().message());
        co_return materialize_result{
          .batches = extents.error(),
          .probe = probe,
        };
    }

    chunked_vector<model::record_batch> results;
    for (auto& e : extents.value()) {
        results.push_back(make_raft_data_batch(std::move(e)));
    }
    co_return materialize_result{
      .batches = std::move(results),
      .probe = probe,
    };
}

} // namespace cloud_topics::l0
