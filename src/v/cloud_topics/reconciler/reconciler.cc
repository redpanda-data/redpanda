/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/reconciler/reconciler.h"

#include "base/source_location.h"
#include "base/vlog.h"
#include "cloud_topics/data_plane_api.h"
#include "cloud_topics/frontend/frontend.h"
#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/metastore/metastore.h"
#include "cloud_topics/level_one/metastore/retry.h"
#include "cloud_topics/log_reader_config.h"
#include "cloud_topics/reconciler/reconciliation_consumer.h"
#include "cloud_topics/reconciler/reconciliation_source.h"
#include "cluster/partition.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "ssx/future-util.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/shared_ptr.hh>
#include <seastar/util/log.hh>

#include <algorithm>
#include <chrono>
#include <exception>
#include <expected>
#include <iterator>
#include <random>

using namespace std::chrono_literals;

namespace cloud_topics::reconciler {

namespace {
ss::logger lg("reconciler");

void log_error(
  const reconcile_error& err,
  vlog::file_line file_line = vlog::file_line::current()) {
    lg.log(
      err.benign ? ss::log_level::debug : ss::log_level::error,
      "{} - {}",
      file_line,
      err.message);
}

} // namespace

reconciler::reconciler(l1::io* l1_io, l1::metastore* metastore)
  : _l1_io(l1_io)
  , _metastore(metastore)
  , _scheduler(
      config::shard_local_cfg().cloud_topics_reconciliation_min_interval.bind(),
      config::shard_local_cfg().cloud_topics_reconciliation_max_interval.bind(),
      config::shard_local_cfg()
        .cloud_topics_reconciliation_target_fill_ratio.bind(),
      config::shard_local_cfg()
        .cloud_topics_reconciliation_speedup_blend.bind(),
      config::shard_local_cfg()
        .cloud_topics_reconciliation_slowdown_blend.bind(),
      config::shard_local_cfg()
        .cloud_topics_reconciliation_max_object_size.bind()) {}

ss::future<> reconciler::start() {
    _probe.setup_metrics();
    ssx::spawn_with_gate(_gate, [this] { return reconciliation_loop(); });
    co_return;
}

ss::future<> reconciler::stop() {
    _as.request_abort();
    co_await _gate.close();
}

void reconciler::attach_partition(
  const model::ntp& ntp,
  model::topic_id_partition tidp,
  data_plane_api* data_plane,
  ss::lw_shared_ptr<cluster::partition> partition) {
    attach_source(make_source(ntp, tidp, data_plane, std::move(partition)));
}

void reconciler::attach_source(ss::shared_ptr<source> src) {
    if (_sources.contains(src->ntp())) {
        return;
    }
    vlog(
      lg.debug,
      "Attaching partition {} (tidp: {})",
      src->ntp(),
      src->topic_id_partition());
    _sources.emplace(src->ntp(), src);
}

void reconciler::detach(const model::ntp& ntp) {
    if (auto it = _sources.find(ntp); it != _sources.end()) {
        vlog(lg.debug, "Detaching partition {}", ntp);
        /*
         * This upcall doesn't synchronize with the rest of the reconciler,
         * which means that once a reference to a source is held,
         * it shouldn't be assumed that the source remains in the
         * _sources collection.
         */
        _sources.erase(it);
    }
}

ss::future<> reconciler::reconciliation_loop() {
    /*
     * Polling is not particularly efficient, and in practice, we'll probably
     * want to look into receiving upcalls from partitions announcing that new
     * data is available.
     * TODO: Investigate performance of polling and alternatives to polling.
     */

    auto deferred = ss::defer(
      [] { vlog(lg.debug, "Reconciliation loop exiting"); });
    ss::lowres_clock::duration next_wait = _scheduler.current_interval();
    while (!_gate.is_closed()) {
        try {
            co_await ss::sleep_abortable(next_wait, _as);
        } catch (const ss::sleep_aborted&) {
            // If the sleep was aborted, we can exit our loop
            co_return;
        }

        if (config::shard_local_cfg()
              .cloud_topics_disable_reconciliation_loop()) {
            vlog(lg.debug, "Reconciliation loop disabled, skipping iteration");
            next_wait = config::shard_local_cfg()
                          .cloud_topics_reconciliation_max_interval.value();
            continue;
        }

        // clang-format off
        /*
         * Error Handling
         * (Props to Claude for the tree diagram)
         *
         * The reconciler uses nested exception boundaries to ensure proper
         * cleanup and partial failure recovery. One object's failure prevents
         * reconciliation of its partitions for this round, but not other
         * partitions'.
         *
         * reconciliation_loop()
         * └─ try/catch → catches unhandled exceptions, logs, better luck next time
         *    └─ reconcile()
         *       └─ FOR EACH OBJECT:
         *          └─ as_future(reconcile_partitions) → catches all exceptions
         *             └─ reconcile_partitions(oid, partitions)
         *                ├─ make_context() → returns error if staging fails
         *                │  └─ as_future(output_stream()) → catches, cleans staging
         *                │
         *                ├─ as_future(build_and_put_object)
         *                │  ├─ build_object() → can throw in builder->finish()
         *                │  └─ put_object() → returns error on upload failure
         *                │
         *                └─ GUARANTEED CLEANUP (always executed):
         *                   ├─ ctx.close_builder()   → closes object builder
         *                   └─ ctx.cleanup_staging() → removes temp file
         *
         * Failure Scopes:
         * - Single object failures:
         *   • Staging file creation fails
         *   • No data in partitions (empty object)
         *   • Build or upload exceptions
         *   • Individual partition read failures
         *   • Object-level metadata failure
         *
         * - Reconciliation round failures:
         *   • Final metastore batch commit failure
         *   • Any unhandled exception in reconcile()
         *
         * - Reconciliation loop termination:
         *   • Shutdown exceptions only
         *
         * Resource Guarantees:
         * - Builder is ALWAYS closed if created
         * - Staging file is ALWAYS removed if created
         * - Failures in one object don't leak resources or affect others
         * - Reconciliation will try again next schedule point after failure,
         *   except for shutdown
         */
        // clang-format on
        try {
            co_await reconcile();
        } catch (...) {
            const auto is_shutdown = ssx::is_shutdown_exception(
              std::current_exception());
            vlogl(
              lg,
              is_shutdown ? ss::log_level::debug : ss::log_level::info,
              "Recoverable error during reconciliation: {}",
              std::current_exception());
        }
        next_wait = _scheduler.current_interval();
    }
}

ss::future<> reconciler::reconcile() {
    _probe.increment_rounds();

    chunked_vector<ss::shared_ptr<source>> sources;
    // Make a copy of the sources to not worry about concurrent modification.
    for (auto& [_, src] : _sources) {
        sources.push_back(src);
    }
    vlog(
      lg.debug,
      "Reconciliation loop tick with {} attached partitions",
      sources.size());
    if (sources.empty()) {
        co_return;
    }

    auto source_sets = partition_sources_into_sets(std::move(sources));

    // Adapt scheduling interval based on max object size produced.
    // Note that we slow down if there's nothing to reconcile or if all
    // objects failed. This is a sort of retry with backoff mechanism.
    size_t max_bytes_produced = 0;
    for (auto& source_set : source_sets) {
        auto bytes = co_await reconcile_source_set(std::move(source_set));
        max_bytes_produced = std::max(max_bytes_produced, bytes);
    }

    _scheduler.adapt(max_bytes_produced);
}

chunked_vector<chunked_vector<ss::shared_ptr<source>>>
reconciler::partition_sources_into_sets(
  chunked_vector<ss::shared_ptr<source>> sources) {
    chunked_hash_map<model::topic_id, chunked_vector<ss::shared_ptr<source>>>
      topic_id_to_sources;
    for (auto& src : sources) {
        auto& src_vec = topic_id_to_sources[src->topic_id_partition().topic_id];
        src_vec.push_back(std::move(src));
    }

    vlog(
      lg.debug,
      "Partitioned sources into {} sets by topic_id",
      topic_id_to_sources.size());

    chunked_vector<chunked_vector<ss::shared_ptr<source>>> result;
    result.reserve(topic_id_to_sources.size());
    for (auto& [_, src_vec] : topic_id_to_sources) {
        result.push_back(std::move(src_vec));
    }
    return result;
}

ss::future<size_t> reconciler::reconcile_source_set(
  chunked_vector<ss::shared_ptr<source>> sources) {
    if (sources.empty()) {
        co_return 0;
    }

    // Begin by creating the set of objects to be built.
    retry_chain_node rtc = l1::make_default_metastore_rtc(_as);
    auto metadata_builder_res = co_await l1::retry_metastore_op(
      [this]() {
          return _metastore->object_builder().then(
            [this](
              std::expected<
                std::unique_ptr<l1::metastore::object_metadata_builder>,
                l1::metastore::errc> result) {
                if (
                  !result.has_value()
                  && result.error() == l1::metastore::errc::transport_error) {
                    _probe.increment_metastore_retries();
                }
                return result;
            });
      },
      rtc);
    if (!metadata_builder_res.has_value()) {
        vlog(
          lg.warn,
          "Could not create object metadata builder: {}",
          metadata_builder_res.error());
        _probe.increment_rounds_failed();
        co_return 0;
    }
    auto& metadata_builder = metadata_builder_res.value();
    chunked_hash_map<l1::object_id, chunked_vector<ss::shared_ptr<source>>>
      oid_to_sources;
    for (const auto& src : sources) {
        auto oid = metadata_builder->get_or_create_object_for(
          src->topic_id_partition());
        if (!oid.has_value()) {
            vlog(lg.warn, "Could not get object: {}", oid.error());
            _probe.increment_rounds_failed();
            co_return 0;
        }
        oid_to_sources[oid.value()].push_back(src);
    }

    // Process sources by their object in parallel.
    chunked_vector<l1::object_id> oids;
    chunked_vector<
      ss::future<std::expected<built_object_metadata, reconcile_error>>>
      futures;
    oids.reserve(oid_to_sources.size());
    futures.reserve(oid_to_sources.size());
    for (const auto& [oid, srcs] : oid_to_sources) {
        oids.push_back(oid);
        futures.push_back(reconcile_sources(oid, srcs));
    }
    // Unbounded concurrency because #futures = #domains, which is small (3).
    auto results = co_await ss::when_all(futures.begin(), futures.end());

    // Process results.
    chunked_vector<built_object_metadata> successful_objects;
    chunked_vector<l1::object_id> failed_objects;
    for (size_t i = 0; i < results.size(); ++i) {
        auto& oid = oids[i];
        auto& object_fut = results[i];

        if (object_fut.failed()) {
            auto ex = object_fut.get_exception();
            const auto is_shutdown = ssx::is_shutdown_exception(ex);
            vlogl(
              lg,
              is_shutdown ? ss::log_level::debug : ss::log_level::error,
              "Exception reconciling object {}: {}",
              oid,
              ex);
            if (is_shutdown) {
                co_return 0;
            }
            failed_objects.push_back(oid);
            continue;
        }

        auto result = object_fut.get();
        if (!result.has_value()) {
            failed_objects.push_back(oid);
            log_error(result.error());
            continue;
        }

        auto obj_metadata = std::move(result).value();
        auto add_result = add_object_metadata(
          oid, obj_metadata, metadata_builder.get());
        if (!add_result.has_value()) {
            failed_objects.push_back(oid);
            log_error(add_result.error().with_context(
              "adding metadata for object {}", oid));
            continue;
        }

        // Success - collect the metadata for final processing.
        successful_objects.push_back(std::move(obj_metadata));
    }

    for (const auto& oid : failed_objects) {
        auto rm_ret = metadata_builder->remove_pending_object(oid);
        vassert(
          rm_ret.has_value(), "Removing object {} in non-pending state", oid);
    }

    // Calculate the max object size produced for scheduling adaptation.
    size_t max_bytes_produced = 0;
    for (const auto& obj : successful_objects) {
        max_bytes_produced = std::max(
          max_bytes_produced, obj.object_info.size_bytes);
    }

    // Check if we have any successful objects to commit.
    if (successful_objects.empty()) {
        // NB: This doesn't count as failing the round because it may be that
        // all sources are fully reconciled.
        vlog(lg.debug, "No successful objects to commit to metastore");
        co_return 0;
    }

    // Commit all successful objects to the metastore.
    auto commit_result = co_await commit_objects(
      successful_objects, std::move(metadata_builder));
    if (!commit_result.has_value()) {
        log_error(commit_result.error().with_context(
          "Abandoning reconciliation run because the L1 metastore operation "
          "failed"));
        _probe.increment_rounds_failed();
        co_return 0;
    }

    co_return max_bytes_produced;
}

ss::future<std::expected<reconciler::built_object_metadata, reconcile_error>>
reconciler::reconcile_sources(
  const l1::object_id& oid,
  const chunked_vector<ss::shared_ptr<source>>& sources) {
    auto ctx_result = co_await make_context();
    if (!ctx_result.has_value()) {
        co_return std::unexpected(ctx_result.error().with_context(
          "reconciling {} sources into object {}", sources.size(), oid));
    }
    auto ctx = std::move(ctx_result.value());

    auto fut = co_await ss::coroutine::as_future(
      build_and_put_object(oid, ctx, sources));

    // Always cleanup.
    auto close_fut = co_await ss::coroutine::as_future(ctx.close_builder());
    if (close_fut.failed()) {
        auto ex = close_fut.get_exception();
        vlogl(
          lg,
          ssx::is_shutdown_exception(ex) ? ss::log_level::debug
                                         : ss::log_level::warn,
          "Exception while closing builder: {}",
          ex);
    }

    auto cleanup_fut = co_await ss::coroutine::as_future(ctx.cleanup_staging());
    if (cleanup_fut.failed()) {
        auto ex = cleanup_fut.get_exception();
        vlogl(
          lg,
          ssx::is_shutdown_exception(ex) ? ss::log_level::debug
                                         : ss::log_level::warn,
          "Exception while cleaning up staging: {}",
          ex);
    }

    if (fut.failed()) {
        auto ex = fut.get_exception();
        co_return std::unexpected(
          reconcile_error(
            "reconciling {} sources into object {}: {}",
            sources.size(),
            oid,
            ex)
            .mark_benign(ssx::is_shutdown_exception(ex)));
    }

    auto result = fut.get();
    if (!result.has_value()) {
        co_return std::unexpected(result.error().with_context(
          "reconciling {} sources into object {}", sources.size(), oid));
    }

    co_return result;
}

ss::future<std::expected<reconciler::built_object_metadata, reconcile_error>>
reconciler::build_and_put_object(
  const l1::object_id& oid,
  builder_context& ctx,
  const chunked_vector<ss::shared_ptr<source>>& sources) {
    // Build the object.
    auto build_result = co_await build_object(ctx, sources);
    if (!build_result.has_value()) {
        _probe.increment_object_build_failed();
        co_return std::unexpected(build_result.error());
    }

    auto obj_meta = std::move(build_result.value());
    if (obj_meta.commits.empty()) {
        _probe.increment_empty_objects_skipped();
        co_return std::unexpected(
          reconcile_error("Skipping put for object {}: no data", oid));
    }

    // Upload the object.
    auto put_result = co_await put_object(oid, ctx);
    if (!put_result.has_value()) {
        _probe.increment_object_upload_failed();
        co_return std::unexpected(put_result.error());
    }

    _probe.increment_objects_uploaded();
    _probe.add_bytes_reconciled(obj_meta.object_info.size_bytes);
    _probe.record_object_size_bytes(obj_meta.object_info.size_bytes);
    _probe.record_sources_per_object(obj_meta.commits.size());

    co_return obj_meta;
}

ss::future<std::expected<reconciler::builder_context, reconcile_error>>
reconciler::make_context() {
    builder_context ctx;

    // Create staging file.
    auto staging_result = co_await _l1_io->create_tmp_file();
    if (!staging_result.has_value()) {
        co_return std::unexpected(
          reconcile_error(
            "Failed to create staging file: {}", staging_result.error())
            .non_benign());
    }
    ctx.staging = std::move(staging_result).value();

    // Create output stream and builder.
    auto stream_fut = co_await ss::coroutine::as_future(
      ctx.staging->output_stream());
    if (stream_fut.failed()) {
        auto ex = stream_fut.get_exception();
        co_await ctx.cleanup_staging();
        co_return std::unexpected(
          reconcile_error("Failed to create output stream: {}", ex)
            .mark_benign(ssx::is_shutdown_exception(ex)));
    }

    auto output_stream = stream_fut.get();
    ctx.builder = l1::object_builder::create(
      std::move(output_stream), l1::object_builder::options{});
    ctx.size_budget
      = config::shard_local_cfg().cloud_topics_reconciliation_max_object_size();

    co_return ctx;
}

ss::future<std::expected<reconciler::built_object_metadata, reconcile_error>>
reconciler::build_object(
  builder_context& ctx, const chunked_vector<ss::shared_ptr<source>>& sources) {
    const auto max_size = ctx.size_budget;

    chunked_vector<commit_info> metas;
    metas.reserve(sources.size());
    for (const auto& src : sources) {
        if (_as.abort_requested()) {
            co_return std::unexpected(
              reconcile_error("abort requested while building object"));
        }

        // Enforce the size limit, but always allow one partition in.
        auto current_size = ctx.builder->file_size();
        if (!metas.empty() && current_size >= max_size) {
            vlog(
              lg.debug,
              "Stopping object build: size {} >= max {}",
              current_size,
              max_size);
            break;
        }
        // Beware underflow if the first partition sneaks a batch in over the
        // size limit.
        ctx.size_budget = current_size >= max_size ? 0
                                                   : max_size - current_size;
        auto start_offset = kafka::next_offset(src->last_reconciled_offset());
        auto read_result = co_await add_source_to_object(
          ctx, src, start_offset);

        if (!read_result.has_value()) {
            // Log an error, we don't want a single stuck partition to
            // prevent all partitions from being reconciled.
            log_error(read_result.error().with_context(
              "unable to reconcile partition {}", src->ntp()));
            continue;
        }
        auto meta = read_result.value();
        if (meta.has_value()) {
            _probe.increment_partitions_reconciled();
            _probe.add_batches_reconciled(meta->batch_count);
            metas.emplace_back(src, std::move(meta).value(), start_offset);
        }
    }
    metas.shrink_to_fit();

    auto obj_info = co_await ctx.builder->finish().finally(
      [&ctx] { return ctx.close_builder(); });
    vlog(
      lg.debug,
      "Built L1 object from {} partitions ({} partitions were skipped)",
      metas.size(),
      sources.size() - metas.size());
    co_return built_object_metadata{
      .object_info = std::move(obj_info),
      .commits = std::move(metas),
    };
}

ss::future<std::expected<void, reconcile_error>>
reconciler::put_object(const l1::object_id& oid, builder_context& ctx) {
    auto metrics_duration = _probe.measure_object_upload_duration();
    auto put_result = co_await _l1_io->put_object(oid, ctx.staging.get(), &_as);
    if (!put_result.has_value()) {
        co_return std::unexpected(reconcile_error(
          "failed to put L1 object {}: {}", oid, put_result.error()));
    }
    vlog(lg.debug, "Successfully put L1 object {}", oid);
    co_return std::expected<void, reconcile_error>{};
}

ss::future<std::expected<std::optional<consumer_metadata>, reconcile_error>>
reconciler::add_source_to_object(
  builder_context& ctx,
  ss::shared_ptr<source> src,
  kafka::offset start_offset) {
    vlog(
      lg.debug,
      "Processing partition {} with LRO {}",
      src->ntp(),
      src->last_reconciled_offset());

    std::optional<consumer_metadata> metadata;
    try {
        auto reader = co_await src->make_reader(
          source::reader_config{
            .start_offset = start_offset,
            .max_bytes = ctx.size_budget,
            .as = &_as,
          });
        metadata = co_await build_from_reader(
          src->topic_id_partition(),
          std::move(reader),
          ctx.builder.get(),
          &_probe);
    } catch (...) {
        co_return std::unexpected(reconcile_error(
          "unable to consume from L0 partition: {}", std::current_exception()));
    }

    if (!metadata.has_value()) {
        vlog(
          lg.debug,
          "No batches found for partition {}",
          src->topic_id_partition());
        co_return std::nullopt;
    }

    vlog(
      lg.debug,
      "Adding partition {} to L1 object with offsets {}~{} starting at offset "
      "{}",
      src->topic_id_partition(),
      metadata->base_offset,
      metadata->last_offset,
      start_offset);

    co_return metadata.value();
}

std::expected<void, reconcile_error> reconciler::add_object_metadata(
  const l1::object_id& oid,
  const built_object_metadata& obj_meta,
  l1::metastore::object_metadata_builder* meta_builder) {
    // Add metadata for this object to the metadata builder.
    // Remember that there are two kinds of partitions here: the
    // partition of the cloud topic and the partition of the L1 object.
    // There may be multiple partitions in the L1 object for a cloud
    // topic partition, but right now we only add one per reconciler run.
    for (const auto& commit : obj_meta.commits) {
        auto [first, last] = obj_meta.object_info.index.partitions.equal_range(
          commit.source->topic_id_partition());
        vassert(
          std::distance(first, last) == 1,
          "expected a single partition in the object");
        for (auto it = first; it != last; ++it) {
            const auto& obj_partition = it->second;
            auto add_result = meta_builder->add(
              oid,
              l1::metastore::object_metadata::ntp_metadata{
                .tidp = commit.source->topic_id_partition(),
                // Use the start offset, this may differ from the base offset in
                // the metadata info in topics with transactions, as control
                // batch offsets are skipped.
                .base_offset = commit.start_offset,
                .last_offset = obj_partition.last_offset,
                .max_timestamp = obj_partition.max_timestamp,
                .pos = obj_partition.file_position,
                .size = obj_partition.length});
            if (!add_result.has_value()) {
                // TODO: The object has been uploaded. The reconciler could
                //       attempt cleanup (or notify a cleanup subsystem).
                return std::unexpected(reconcile_error(
                  "Failed to finish metadata for partition {} of object {}: {}",
                  commit.source->topic_id_partition(),
                  oid,
                  add_result.error()));
            }
        }
    }

    auto meta_result = meta_builder->finish(
      oid, obj_meta.object_info.footer_offset, obj_meta.object_info.size_bytes);
    if (!meta_result.has_value()) {
        // TODO: The object has been uploaded. The reconciler could
        //       attempt cleanup (or notify a cleanup subsystem).
        return std::unexpected(reconcile_error(
                                 "Failed to finish metadata for object {}: {}",
                                 oid,
                                 meta_result.error())
                                 .non_benign());
    }

    return {};
}

ss::future<std::expected<void, reconcile_error>> reconciler::commit_objects(
  const chunked_vector<built_object_metadata>& objects,
  std::unique_ptr<l1::metastore::object_metadata_builder> meta_builder) {
    // It's possible to build the terms map as we build the objects, but
    // I think re-iterating over all the commits here is worth
    // it in exchange for less context passing among functions.
    l1::metastore::term_offset_map_t terms;
    for (const auto& obj_meta : objects) {
        for (const auto& commit : obj_meta.commits) {
            auto tidp = commit.source->topic_id_partition();
            chunked_vector<l1::metastore::term_offset> term_offsets;
            auto it = commit.metadata.terms.begin();
            auto end = commit.metadata.terms.end();
            vassert(
              it != end, "missing terms for ntp: {}", commit.source->ntp());
            // We have to override the start term to be the start offset, this
            // may differ from the first observed offset when transaction
            // control batches are skipped but still need to be accounted for in
            // the offset space.
            term_offsets.emplace_back(it->first, commit.start_offset);
            ++it;
            for (const auto& [term, first_offset] :
                 std::ranges::subrange(it, end)) {
                term_offsets.emplace_back(term, first_offset);
            }
            terms[tidp] = std::move(term_offsets);
        }
    }

    retry_chain_node rtc = l1::make_default_metastore_rtc(_as);
    auto add_objects_result = co_await l1::retry_metastore_op(
      [this, &meta_builder, &terms]() {
          auto metrics_duration_add_objects
            = _probe.measure_metastore_add_objects_duration();
          return _metastore->add_objects(*meta_builder, terms)
            .then(
              [this,
               metrics_duration_add_objects = std::move(
                 metrics_duration_add_objects)](
                std::expected<l1::metastore::add_response, l1::metastore::errc>
                  result) mutable {
                  metrics_duration_add_objects.reset();

                  if (
                    !result.has_value()
                    && result.error() == l1::metastore::errc::transport_error) {
                      _probe.increment_metastore_retries();
                  }

                  return result;
              });
      },
      rtc);

    if (!add_objects_result.has_value()) {
        // TODO: The objects have been uploaded. The reconciler could
        //       attempt cleanup (or notify a cleanup subsystem).
        co_return std::unexpected(reconcile_error(
          "Failed to add objects to the L1 metastore: {}",
          add_objects_result.error()));
    }

    vlog(
      lg.debug,
      "Successfully added {} objects to L1 metastore",
      objects.size());

    // Now update the LRO, taking into account any corrections from
    // the metastore.
    const auto& corrected_next_offsets
      = add_objects_result.value().corrected_next_offsets;
    std::optional<reconcile_error> error;
    for (const auto& obj_meta : objects) {
        for (const auto& commit : obj_meta.commits) {
            auto tidp = commit.source->topic_id_partition();
            kafka::offset lro = commit.metadata.last_offset;
            auto it = corrected_next_offsets.find(tidp);
            if (it != corrected_next_offsets.end()) {
                _probe.increment_offset_corrections();
                // We want the previous offset, because that is what was last
                // reconciled. During next reconciliation we should get the
                // offset *after* the LRO to start reading from.
                lro = kafka::prev_offset(it->second);
            }
            auto result = co_await commit.source->set_last_reconciled_offset(
              lro, _as);
            if (result.has_value()) {
                vlog(
                  lg.debug,
                  "successfully bumped LRO for {} (tidp: {}) to {}",
                  commit.source->ntp(),
                  tidp,
                  lro);
            } else {
                // Don't fail early, just keep going until we're done.
                if (error) {
                    error = error->with_context(
                      "failed to set LRO in L0: {}", result.error());
                } else {
                    error = reconcile_error(
                      "failed to set LRO in L0: {}", result.error());
                }
                if (result.error() == source::errc::failure) {
                    // Other errors can be expected in normal operating
                    // conditions.
                    error = error->non_benign();
                }
            }
        }
    }
    co_return error
      .transform(
        [](reconcile_error& err) -> std::expected<void, reconcile_error> {
            return std::unexpected(std::move(err));
        })
      .value_or(std::expected<void, reconcile_error>{});
}

} // namespace cloud_topics::reconciler
