/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/data_plane_impl.h"

#include "base/outcome.h"
#include "cloud_io/cache_service.h"
#include "cloud_topics/batch_cache/batch_cache.h"
#include "cloud_topics/cluster_services.h"
#include "cloud_topics/data_plane_api.h"
#include "cloud_topics/level_zero/batcher/batcher.h"
#include "cloud_topics/level_zero/cluster_services_impl/cluster_services.h"
#include "cloud_topics/level_zero/pipeline/read_pipeline.h"
#include "cloud_topics/level_zero/pipeline/write_pipeline.h"
#include "cloud_topics/level_zero/read_fanout/read_fanout.h"
#include "cloud_topics/level_zero/read_request_scheduler/read_request_scheduler.h"
#include "cloud_topics/level_zero/reader/fetch_request_handler.h"
#include "cloud_topics/level_zero/write_request_scheduler/write_request_scheduler.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "ssx/sharded_service_container.h"
#include "storage/api.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>

#include <memory>

namespace cloud_topics {

class impl
  : public data_plane_api
  , public ssx::sharded_service_container {
public:
    explicit impl(ss::sstring logger_name)
      : ssx::sharded_service_container(std::move(logger_name)) {}

    ss::future<> construct(
      seastar::sharded<cloud_io::remote>* io,
      seastar::sharded<cloud_io::cache>* cache,
      cloud_storage_clients::bucket_name bucket,
      seastar::sharded<storage::api>* storage_api,
      seastar::sharded<cluster::cluster_epoch_service<ss::lowres_clock>>*
        cluster_services) {
        co_await construct_service(
          _cluster_services, std::ref(*cluster_services));

        co_await construct_service(_write_pipeline);

        co_await construct_service(
          _write_req_scheduler, ss::sharded_parameter([this] {
              return _write_pipeline.local().register_write_pipeline_stage();
          }));

        co_await construct_service(
          _batcher,
          ss::sharded_parameter([this] {
              return _write_pipeline.local().register_write_pipeline_stage();
          }),
          ss::sharded_parameter([bucket] { return bucket; }),
          ss::sharded_parameter([io] { return std::ref(io->local()); }),
          ss::sharded_parameter([this] { return &_cluster_services.local(); }));

        co_await construct_service(_read_pipeline);

        co_await construct_service(
          _read_fanout, ss::sharded_parameter([this] {
              return _read_pipeline.local().register_read_pipeline_stage();
          }));

        if (config::shard_local_cfg().cloud_topics_parallel_fetch_enabled()) {
            co_await construct_service(
              _read_request_scheduler, ss::sharded_parameter([this] {
                  return _read_pipeline.local().register_read_pipeline_stage();
              }));
        }

        co_await construct_service(
          _fetch_handler,
          ss::sharded_parameter([this] {
              return _read_pipeline.local().register_read_pipeline_stage();
          }),
          ss::sharded_parameter([bucket] { return bucket; }),
          ss::sharded_parameter([io] { return &io->local(); }),
          ss::sharded_parameter([cache] { return &cache->local(); }));

        co_await construct_service(
          _batch_cache, ss::sharded_parameter([storage_api] {
              return &storage_api->local().log_mgr();
          }));
    }

    seastar::future<> start() override {
        co_await _write_req_scheduler.invoke_on_all(
          [](auto& s) { return s.start(); });
        co_await _batcher.invoke_on_all([](auto& s) { return s.start(); });
        co_await _read_fanout.invoke_on_all([](auto& s) { return s.start(); });
        if (_read_request_scheduler.local_is_initialized()) {
            co_await _read_request_scheduler.invoke_on_all(
              [](auto& s) { return s.start(); });
        }
        co_await _fetch_handler.invoke_on_all(
          [](auto& s) { return s.start(); });
        co_await _batch_cache.invoke_on_all([](auto& s) { return s.start(); });
    }

    seastar::future<> stop() override {
        co_await _write_pipeline.invoke_on_all(
          [](auto& p) { return p.shutdown(); });
        co_await _read_pipeline.invoke_on_all(
          [](auto& p) { return p.shutdown(); });
        co_await ss::async(
          [this] { ssx::sharded_service_container::shutdown(); });
        co_return;
    }

    ss::future<result<chunked_vector<extent_meta>>> write_and_debounce(
      model::ntp ntp,
      cluster_epoch min_epoch,
      chunked_vector<model::record_batch> r,
      model::timeout_clock::time_point timeout) override {
        auto res = co_await _write_pipeline.local().write_and_debounce(
          std::move(ntp), min_epoch, std::move(r), timeout);
        if (res.has_value()) {
            co_return std::move(res.value());
        }
        co_return res.error();
    }

    ss::future<result<chunked_vector<model::record_batch>>> materialize(
      model::ntp ntp,
      size_t output_size_estimate,
      chunked_vector<extent_meta> metadata,
      model::timeout_clock::time_point timeout) override {
        if (metadata.empty()) {
            co_return chunked_vector<model::record_batch>{};
        }
        auto res = co_await _read_pipeline.local().make_reader(
          ntp,
          {
            .output_size_estimate = output_size_estimate,
            .meta = std::move(metadata),
          },
          timeout);
        if (!res) {
            co_return res.error();
        }
        co_return std::move(res.value().results);
    }

    void cache_put(const model::ntp& ntp, const model::record_batch& b) final {
        _batch_cache.local().put(ntp, b);
    }

    std::optional<model::record_batch>
    cache_get(const model::ntp& ntp, model::offset o) final {
        return _batch_cache.local().get(ntp, o);
    }

private:
    ss::sharded<l0::cluster_services> _cluster_services;
    // Write path
    ss::sharded<l0::write_pipeline<>> _write_pipeline;
    ss::sharded<l0::write_request_scheduler<>> _write_req_scheduler;
    ss::sharded<l0::batcher<>> _batcher;
    // Read path
    ss::sharded<l0::read_pipeline<>> _read_pipeline;
    ss::sharded<l0::read_fanout> _read_fanout;
    ss::sharded<l0::read_request_scheduler> _read_request_scheduler;

    ss::sharded<l0::fetch_handler> _fetch_handler;
    // Batch cache
    ss::sharded<batch_cache> _batch_cache;
};

ss::future<std::unique_ptr<data_plane_api>> make_data_plane(
  ss::sstring logger_name,
  ss::sharded<cloud_io::remote>* remote,
  ss::sharded<cloud_io::cache>* cache,
  cloud_storage_clients::bucket_name bucket,
  ss::sharded<storage::api>* log_manager,
  seastar::sharded<cluster::cluster_epoch_service<>>* cluster_services) {
    auto p = std::make_unique<impl>(std::move(logger_name));
    co_await p->construct(
      remote,
      cache,
      std::move(bucket),
      log_manager,
      std::ref(cluster_services));
    co_return std::move(p);
}

} // namespace cloud_topics
