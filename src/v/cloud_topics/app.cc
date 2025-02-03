/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "cloud_topics/app.h"

#include "cloud_storage/cache_service.h"
#include "cloud_topics/L0_read_path/L0_fetch_handler.h"
#include "cloud_topics/batcher/batcher.h"
#include "cloud_topics/core/read_pipeline.h"
#include "cloud_topics/core/write_pipeline.h"
#include "cloud_topics/interfaces/cluster_partition_manager.h"
#include "cloud_topics/reconciler/reconciler.h"
#include "cloud_topics/throttler/throttler.h"

#include <memory>

namespace experimental::cloud_topics {

/*TODO: remove*/
static ss::logger dbg_log("cloud_topics_debug");
class app::impl {
public:
    impl(
      seastar::sharded<cluster::partition_manager>* pm,
      seastar::sharded<cloud_io::remote>* io,
      seastar::sharded<cloud_storage::cache>* cache,
      cloud_storage_clients::bucket_name bucket)
      : _reconciler(std::make_unique<reconciler::reconciler>(pm, io))
      , _write_pipeline(std::make_unique<core::write_pipeline<>>())
      , _throttler(std::make_unique<throttler<>>(
          10_MiB /*TODO: fixme*/,
          _write_pipeline->register_write_pipeline_stage()))
      , _batcher(std::make_unique<batcher<>>(
          _write_pipeline->register_write_pipeline_stage(),
          bucket,
          io->local()))
      , _read_pipeline(std::make_unique<core::read_pipeline<>>())
      , _l0_resolver(std::make_unique<l0_fetch_handler>(
          _read_pipeline->register_read_pipeline_stage(),
          bucket,
          &io->local(),
          &cache->local(),
          make_cluster_partition_manager(pm->local()))) {}

    seastar::future<> start() {
        // Reconciler
        co_await _reconciler->start();
        // Write path
        co_await _throttler->start();
        co_await _batcher->start();
        // Read path
        co_await _l0_resolver->start();
    }
    seastar::future<> stop() {
        // Read path
        co_await _read_pipeline->stop();
        co_await _l0_resolver->stop();
        // Write path
        co_await _write_pipeline->stop();
        co_await _batcher->stop();
        co_await _throttler->stop();
        // Reconciler
        co_await _reconciler->stop();
    }

private:
    std::unique_ptr<reconciler::reconciler> _reconciler;
    // Write path
    std::unique_ptr<core::write_pipeline<>> _write_pipeline;
    std::unique_ptr<throttler<>> _throttler;
    std::unique_ptr<batcher<>> _batcher;
    // Read path
    std::unique_ptr<core::read_pipeline<>> _read_pipeline;
    std::unique_ptr<l0_fetch_handler> _l0_resolver;
};

app::app(
  seastar::sharded<cluster::partition_manager>* partition_manager,
  seastar::sharded<cloud_io::remote>* remote,
  seastar::sharded<cloud_storage::cache>* cache,
  cloud_storage_clients::bucket_name bucket)
  : _impl(std::make_unique<impl>(
      partition_manager, remote, cache, std::move(bucket))) {}

app::~app() = default;

seastar::future<> app::start() { return _impl->start(); }

seastar::future<> app::stop() { return _impl->stop(); }

} // namespace experimental::cloud_topics
