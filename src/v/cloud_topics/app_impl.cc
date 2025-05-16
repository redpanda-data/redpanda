/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "cloud_topics/app_impl.h"

#include "base/outcome.h"
#include "cloud_storage/cache_service.h"
#include "cloud_topics/batcher/batcher.h"
#include "cloud_topics/core/read_pipeline.h"
#include "cloud_topics/core/write_pipeline.h"
#include "cloud_topics/read_path/fetch_request_handler.h"
#include "cloud_topics/reconciler/reconciler.h"
#include "cloud_topics/throttler/throttler.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "storage/types.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>

#include <memory>

namespace experimental::cloud_topics {

class impl
  : public api
  , public ss::enable_shared_from_this<impl> {
public:
    impl(
      seastar::sharded<cluster::partition_manager>* pm,
      seastar::sharded<cloud_io::remote>* io,
      seastar::sharded<cloud_storage::cache>* cache,
      cloud_storage_clients::bucket_name bucket)
      : _reconciler(
          std::make_unique<reconciler::reconciler>(shared_from_this(), pm, io))
      , _write_pipeline(std::make_unique<core::write_pipeline<>>())
      , _throttler(std::make_unique<throttler<>>(
          10_MiB /*TODO: fixme*/,
          _write_pipeline->register_write_pipeline_stage()))
      , _batcher(std::make_unique<batcher<>>(
          _write_pipeline->register_write_pipeline_stage(),
          bucket,
          io->local()))
      , _read_pipeline(std::make_unique<core::read_pipeline<>>())
      , _l0_resolver(std::make_unique<fetch_handler>(
          _read_pipeline->register_read_pipeline_stage(),
          bucket,
          &io->local(),
          &cache->local())) {}

    seastar::future<> start() override {
        // Reconciler
        co_await _reconciler->start();
        // Write path
        co_await _throttler->start();
        co_await _batcher->start();
        // Read path
        co_await _l0_resolver->start();
    }
    seastar::future<> stop() override {
        // Read path
        co_await _read_pipeline->stop();
        co_await _l0_resolver->stop();
        // Write path
        co_await _write_pipeline->stop();
        co_await _batcher->stop();
        co_await _throttler->stop();
        //  Reconciler
        co_await _reconciler->stop();
    }

    ss::future<result<ss::circular_buffer<model::record_batch>>>
    write_and_debounce(
      model::ntp ntp,
      model::record_batch_reader r,
      std::chrono::milliseconds timeout) override {
        return _write_pipeline->write_and_debounce(
          std::move(ntp), std::move(r), timeout);
    }

    ss::future<result<ss::circular_buffer<model::record_batch>>> materialize(
      model::ntp ntp,
      size_t output_size_estimate,
      ss::circular_buffer<model::record_batch> metadata,
      std::chrono::milliseconds timeout) override {
        auto res = co_await _read_pipeline->make_reader(
          ntp,
          {.output_size_estimate = output_size_estimate,
           .meta = std::move(metadata)},
          timeout);
        if (!res) {
            co_return res.error();
        }
        co_return std::move(res.value().results);
    }

private:
    std::unique_ptr<reconciler::reconciler> _reconciler;
    // Write path
    std::unique_ptr<core::write_pipeline<>> _write_pipeline;
    std::unique_ptr<throttler<>> _throttler;
    std::unique_ptr<batcher<>> _batcher;
    // Read path
    std::unique_ptr<core::read_pipeline<>> _read_pipeline;
    std::unique_ptr<fetch_handler> _l0_resolver;
};

ss::shared_ptr<api> make_app(
  ss::sharded<cluster::partition_manager>* partition_manager,
  ss::sharded<cloud_io::remote>* remote,
  ss::sharded<cloud_storage::cache>* cache,
  cloud_storage_clients::bucket_name bucket) {
    return ss::make_shared<impl>(
      partition_manager, remote, cache, std::move(bucket));
}

} // namespace experimental::cloud_topics
