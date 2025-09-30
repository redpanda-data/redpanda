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
#include "cloud_topics/log_reader_config.h"
#include "cloud_topics/reconciler/reconciliation_consumer.h"
#include "cloud_topics/reconciler/reconciliation_source.h"
#include "cluster/partition.h"
#include "model/fundamental.h"
#include "ssx/future-util.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/shared_ptr.hh>
#include <seastar/util/log.hh>

#include <chrono>
#include <expected>

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
  , _metastore(metastore) {}

ss::future<> reconciler::start() {
    ssx::spawn_with_gate(_gate, [this] { return reconciliation_loop(); });
    co_return;
}

ss::future<> reconciler::stop() {
    _as.request_abort();
    _control_sem.broken();

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
    constexpr std::chrono::seconds poll_frequency(10);

    while (!_gate.is_closed()) {
        try {
            co_await _control_sem.wait(
              poll_frequency, std::max(_control_sem.current(), size_t(1)));
        } catch (const ss::semaphore_timed_out& ex) {
            // Time to do some work.
            std::ignore = ex;
        }

        vlog(
          lg.debug,
          "Reconciliation loop tick with {} attached partitions",
          _sources.size());

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
    }
}

ss::future<> reconciler::reconcile() {
    chunked_vector<ss::shared_ptr<source>> sources;
    // Make a copy of the sources to not worry about concurrent modification.
    for (auto& [_, src] : _sources) {
        sources.push_back(src);
    }
    if (sources.empty()) {
        vlog(lg.trace, "No leader partitions to reconcile");
        co_return;
    }

    // Begin by creating the set of objects to be built.
    auto metadata_builder_res = co_await _metastore->object_builder();
    if (!metadata_builder_res.has_value()) {
        vlog(
          lg.warn,
          "Could not create object metadata builder: {}",
          metadata_builder_res.error());
        co_return;
    }
    auto& metadata_builder = metadata_builder_res.value();
    chunked_hash_map<l1::object_id, chunked_vector<ss::shared_ptr<source>>>
      oid_to_sources;
    for (const auto& src : sources) {
        auto oid = metadata_builder->get_or_create_object_for(
          src->topic_id_partition());
        if (!oid.has_value()) {
            vlog(lg.warn, "Could not get object: {}", oid.error());
            co_return;
        }
        oid_to_sources[oid.value()].push_back(src);
    }

    // Process sources by their object. This should be easier to
    // improve than processing source-by-source.
    chunked_vector<built_object_metadata> successful_objects;
    chunked_vector<l1::object_id> failed_objects;
    for (const auto& [oid, sources] : oid_to_sources) {
        if (_as.abort_requested()) {
            co_return;
        }
        auto object_fut = co_await ss::coroutine::as_future(
          reconcile_sources(oid, sources));
        if (object_fut.failed()) {
            auto ex = object_fut.get_exception();
            const auto is_shutdown = ssx::is_shutdown_exception(ex);
            vlogl(
              lg,
              is_shutdown ? ss::log_level::debug : ss::log_level::error,
              "Exception reconciling {} partitions into object {}: {}",
              sources.size(),
              oid,
              ex);
            if (is_shutdown) {
                co_return;
            }
            failed_objects.push_back(oid);
            continue; // Skip this object and move to the next
        }

        auto result = object_fut.get();
        if (!result.has_value()) {
            failed_objects.push_back(oid);
            log_error(result.error().with_context(
              "Exception reconciling {} partitions into object {}",
              sources.size(),
              oid));
            continue; // Skip this object and move to the next
        }

        auto obj_metadata = std::move(result).value();
        auto add_result = add_object_metadata(
          oid, obj_metadata, metadata_builder.get());
        if (!add_result.has_value()) {
            failed_objects.push_back(oid);
            log_error(add_result.error().with_context(
              "Exception reconciling {} partitions into object {}",
              sources.size(),
              oid));
            continue; // Skip this object and move to the next
        }

        // Success - collect the metadata for final processing.
        successful_objects.push_back(std::move(obj_metadata));
    }

    for (const auto& oid : failed_objects) {
        auto rm_ret = metadata_builder->remove_pending_object(oid);
        vassert(
          rm_ret.has_value(), "Removing object {} in non-pending state", oid);
    }

    // Check if we have any successful objects to commit.
    if (successful_objects.empty()) {
        vlog(lg.debug, "No successful objects to commit to metastore");
        co_return;
    }

    // Commit all successful objects to the metastore.
    auto commit_result = co_await commit_objects(
      successful_objects, std::move(metadata_builder));
    if (!commit_result.has_value()) {
        log_error(commit_result.error().with_context(
          "Abandoning reconciliation run because the L1 metastore operation "
          "failed"));
        co_return;
    }
}

ss::future<std::expected<reconciler::built_object_metadata, reconcile_error>>
reconciler::reconcile_sources(
  const l1::object_id& oid,
  const chunked_vector<ss::shared_ptr<source>>& sources) {
    auto ctx_result = co_await make_context();
    if (!ctx_result.has_value()) {
        co_return std::unexpected(ctx_result.error());
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
            "Exception building and putting object {}: {}", oid, ex)
            .mark_benign(ssx::is_shutdown_exception(ex)));
    }

    co_return fut.get();
}

ss::future<std::expected<reconciler::built_object_metadata, reconcile_error>>
reconciler::build_and_put_object(
  const l1::object_id& oid,
  builder_context& ctx,
  const chunked_vector<ss::shared_ptr<source>>& sources) {
    // Build the object.
    auto build_result = co_await build_object(ctx, sources);
    if (!build_result.has_value()) {
        co_return std::unexpected(build_result.error());
    }

    auto obj_meta = std::move(build_result.value());
    if (obj_meta.commits.empty()) {
        co_return std::unexpected(
          reconcile_error("Skipping put for object {}: no data", oid));
    }

    // Upload the object.
    auto put_result = co_await put_object(oid, ctx);
    if (!put_result.has_value()) {
        co_return std::unexpected(put_result.error());
    }

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

    co_return ctx;
}

ss::future<std::expected<reconciler::built_object_metadata, reconcile_error>>
reconciler::build_object(
  builder_context& ctx, const chunked_vector<ss::shared_ptr<source>>& sources) {
    chunked_vector<commit_info> metas;
    metas.reserve(sources.size());
    for (const auto& src : sources) {
        if (_as.abort_requested()) {
            co_return std::unexpected(
              reconcile_error("abort requested while building object"));
        }
        auto meta = co_await add_source_to_object(ctx, src);
        if (meta.has_value()) {
            metas.emplace_back(src, std::move(meta).value());
        }
    }
    metas.shrink_to_fit();

    auto obj_info = co_await ctx.builder->finish().finally(
      [&ctx] { return ctx.close_builder(); });
    vlog(
      lg.debug,
      "Built L1 object from {} partitions ({} partitions didn't fit)",
      metas.size(),
      sources.size() - metas.size());
    co_return built_object_metadata{
      .object_info = std::move(obj_info),
      .commits = std::move(metas),
    };
}

ss::future<std::expected<void, reconcile_error>>
reconciler::put_object(const l1::object_id& oid, builder_context& ctx) {
    auto put_result = co_await _l1_io->put_object(oid, ctx.staging.get(), &_as);
    if (!put_result.has_value()) {
        co_return std::unexpected(reconcile_error(
          "failed to put L1 object {}: {}", oid, put_result.error()));
    }
    vlog(lg.debug, "Successfully put L1 object {}", oid);
    co_return std::expected<void, reconcile_error>{};
}

ss::future<std::optional<consumer_metadata>> reconciler::add_source_to_object(
  builder_context& ctx, ss::shared_ptr<source> src) {
    vlog(
      lg.debug,
      "Processing partition {} with LRO {}",
      src->ntp(),
      src->last_reconciled_offset());

    // Since the LRO is the last thing inclusive of what we uploaded to L1, we
    // want to start reading from the next offset.
    auto start_offset = kafka::next_offset(src->last_reconciled_offset());
    auto reader = co_await src->make_reader(cloud_topic_log_reader_config(
      /*start_offset=*/start_offset,
      /*max_offset=*/kafka::offset::max(),
      /*min_bytes=*/1,
      /*max_bytes=*/ctx.size_budget,
      /*type_filter=*/std::nullopt,
      /*time=*/std::nullopt,
      /*as=*/_as));
    reconciliation_consumer consumer(
      ctx.builder.get(), src->topic_id_partition());
    auto metadata = co_await std::move(reader).consume(
      std::move(consumer), model::no_timeout);

    if (!metadata.has_value()) {
        vlog(
          lg.debug,
          "No batches found for partition {}",
          src->topic_id_partition());
        co_return std::nullopt;
    }

    vlog(
      lg.debug,
      "Adding partition {} to L1 object with offsets {}~{}",
      src->topic_id_partition(),
      metadata->base_offset,
      metadata->last_offset);

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
    // topic partition.
    for (const auto& commit : obj_meta.commits) {
        auto [first, last] = obj_meta.object_info.index.partitions.equal_range(
          commit.source->topic_id_partition());
        for (auto it = first; it != last; ++it) {
            const auto& obj_partition = it->second;
            auto add_result = meta_builder->add(
              oid,
              l1::metastore::object_metadata::ntp_metadata{
                .tidp = commit.source->topic_id_partition(),
                .base_offset = obj_partition.first_offset,
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

ss::future<std::expected<l1::metastore::add_response, reconcile_error>>
reconciler::add_objects_with_retry(
  std::unique_ptr<l1::metastore::object_metadata_builder> meta_builder,
  l1::metastore::term_offset_map_t terms) {
    static constexpr auto timeout = 5s;
    static constexpr auto backoff = 100ms;

    retry_chain_node rtc(_as, ss::lowres_clock::now() + timeout, backoff);
    retry_chain_logger ctxlog(lg, rtc, "add_objects");
    for (auto permit = rtc.retry(); permit.is_allowed; permit = rtc.retry()) {
        auto add_result = co_await _metastore->add_objects(
          *meta_builder, terms);

        if (add_result.has_value()) {
            co_return std::move(add_result).value();
        }

        if (add_result.error() != l1::metastore::errc::transport_error) {
            co_return std::unexpected(reconcile_error(
              "Non-retryable error adding objects to the L1 metastore: {}",
              add_result.error()));
        }

        co_await ss::sleep_abortable(permit.delay, rtc.root_abort_source());
    }

    co_return std::unexpected(reconcile_error(
      "ran out of retries attempt to add objects to L1 metastore"));
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
            for (const auto& [term, first_offset] : commit.metadata.terms) {
                term_offsets.emplace_back(term, first_offset);
            }
            terms[tidp] = std::move(term_offsets);
        }
    }

    auto add_objects_result = co_await add_objects_with_retry(
      std::move(meta_builder), std::move(terms));
    if (!add_objects_result.has_value()) {
        // TODO: The objects have been uploaded. The reconciler could
        //       attempt cleanup (or notify a cleanup subsystem).
        co_return std::unexpected(add_objects_result.error().with_context(
          "Failed to add objects to the L1 metastore"));
    }

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
