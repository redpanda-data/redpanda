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
#include "cloud_topics/level_zero/read_merge/read_merge.h"
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

struct staged_pipeline_write : public staged_write::batch_data {
    l0::write_pipeline<>::prepared_data data;
};

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
        if (config::shard_local_cfg().cloud_topics_fetch_debounce_enabled()) {
            co_await construct_service(
              _read_merge, ss::sharded_parameter([this] {
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
        if (_read_merge.local_is_initialized()) {
            co_await _read_merge.invoke_on_all(
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

    ss::future<std::expected<staged_write, std::error_code>>
    stage_write(chunked_vector<model::record_batch> batches) override {
        auto reservation = co_await _write_pipeline.local().prepare_write(
          std::move(batches));
        if (!reservation.has_value()) {
            co_return std::unexpected(reservation.error());
        }
        auto staged = std::make_unique<staged_pipeline_write>();
        staged->data = std::move(reservation.value());
        co_return staged_write{.staged = std::move(staged)};
    }

    ss::future<std::expected<upload_meta, std::error_code>> execute_write(
      model::ntp ntp,
      cluster_epoch min_epoch,
      staged_write reservation,
      model::timeout_clock::time_point deadline) override {
        auto staged = std::unique_ptr<staged_pipeline_write>(
          static_cast<staged_pipeline_write*>(reservation.staged.release()));
        co_return co_await _write_pipeline.local().execute_write(
          std::move(ntp), min_epoch, std::move(staged->data), deadline);
    }

    ss::future<result<chunked_vector<model::record_batch>>> materialize(
      model::ntp ntp,
      size_t output_size_estimate,
      chunked_vector<extent_meta> metadata,
      model::timeout_clock::time_point timeout,
      model::opt_abort_source_t as,
      allow_materialization_failure allow_mat_failure) override {
        if (metadata.empty()) {
            co_return chunked_vector<model::record_batch>{};
        }
        // Callers must respect materialize_max_bytes() to avoid blocking
        // forever on the read pipeline's memory semaphore.
        auto max_bytes = materialize_max_bytes();
        vassert(
          output_size_estimate <= max_bytes,
          "materialize request size {} exceeds limit {}; caller must "
          "respect materialize_max_bytes()",
          output_size_estimate,
          max_bytes);
        auto res = co_await _read_pipeline.local().make_reader(
          ntp,
          {
            .output_size_estimate = output_size_estimate,
            .meta = std::move(metadata),
            .allow_mat_failure = allow_mat_failure,
          },
          timeout,
          as);
        if (!res) {
            co_return res.error();
        }
        co_return std::move(res.value().results);
    }

    void cache_put(
      const model::topic_id_partition& tidp,
      const model::record_batch& b) final {
        _batch_cache.local().put(tidp, b);
    }

    std::optional<model::record_batch>
    cache_get(const model::topic_id_partition& tidp, model::offset o) final {
        return _batch_cache.local().get(tidp, o);
    }

    void cache_put_ordered(
      const model::topic_id_partition& tidp,
      chunked_vector<model::record_batch> batches) final {
        _batch_cache.local().put_ordered(tidp, std::move(batches));
    }

    ss::future<> cache_wait(
      const model::topic_id_partition& tidp,
      model::offset offset,
      model::offset last_known,
      model::timeout_clock::time_point deadline,
      std::optional<std::reference_wrapper<ss::abort_source>> as) final {
        return _batch_cache.local().wait_for_offset(
          tidp, offset, last_known, deadline, as);
    }

    size_t materialize_max_bytes() const final {
        return _read_pipeline.local().memory_quota_capacity();
    }

    ss::future<std::optional<cluster_epoch>>
    get_current_epoch(ss::abort_source* as) noexcept final {
        auto epoch_fut = co_await ss::coroutine::as_future(
          _cluster_services.local().current_epoch(as));
        if (epoch_fut.failed()) {
            auto e = epoch_fut.get_exception();
            vlog(_log.warn, "Failed to get cluster epoch: {}", e);
            co_return std::nullopt;
        }
        co_return epoch_fut.get();
    }

    ss::future<> invalidate_epoch_below(cluster_epoch epoch) noexcept final {
        co_await _cluster_services.local().invalidate_epoch_below(epoch);
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
    ss::sharded<l0::read_merge<>> _read_merge;

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
