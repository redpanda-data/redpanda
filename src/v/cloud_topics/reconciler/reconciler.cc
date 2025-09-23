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

#include "base/vlog.h"
#include "cloud_topics/data_plane_api.h"
#include "cloud_topics/frontend/frontend.h"
#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/metastore/metastore.h"
#include "cloud_topics/reconciler/reconciliation_consumer.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition.h"
#include "kafka/utils/txn_reader.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "random/generators.h"
#include "ssx/future-util.h"

#include <seastar/core/shared_ptr.hh>
#include <seastar/util/log.hh>

namespace {
ss::logger lg("reconciler");

bool is_cloud_partition(
  const ss::lw_shared_ptr<cluster::partition>& partition) {
    return partition->get_ntp_config().cloud_topic_enabled();
}

class aborted_transaction_tracker_impl
  : public kafka::aborted_transaction_tracker {
public:
    aborted_transaction_tracker_impl(
      cloud_topics::frontend* fe,
      ss::lw_shared_ptr<const storage::offset_translator_state> translator)
      : _fe(fe)
      , _translator(std::move(translator)) {}

    ss::future<std::vector<model::tx_range>>
    compute_aborted_transactions(model::offset base, model::offset max) final {
        return _fe->aborted_transactions(
          model::offset_cast(base), model::offset_cast(max), _translator);
    }

private:
    cloud_topics::frontend* _fe;
    ss::lw_shared_ptr<const storage::offset_translator_state> _translator;
};

} // namespace

namespace cloud_topics::reconciler {

reconciler::reconciler(
  cluster::partition_manager* pm,
  data_plane_api* data_plane,
  l1::io* l1_io,
  cluster::metadata_cache* metadata_cache,
  l1::metastore* metastore)
  : _partition_manager(pm)
  , _data_plane(data_plane)
  , _l1_io(l1_io)
  , _metadata_cache(metadata_cache)
  , _metastore(metastore) {}

std::optional<model::topic_id_partition>
reconciler::ntp_to_topic_id_partition(const model::ntp& ntp) const {
    model::topic_namespace_view topic_ns_view(ntp);
    auto topic_cfg = _metadata_cache->get_topic_cfg(topic_ns_view);
    if (!topic_cfg.has_value() || !topic_cfg->tp_id.has_value()) {
        return std::nullopt;
    }
    return model::topic_id_partition{*topic_cfg->tp_id, ntp.tp.partition};
}

ss::future<> reconciler::start() {
    _manage_notify_handle = _partition_manager->register_manage_notification(
      model::kafka_namespace, [this](ss::lw_shared_ptr<cluster::partition> p) {
          attach_partition(std::move(p));
      });

    _unmanage_notify_handle
      = _partition_manager->register_unmanage_notification(
        model::kafka_namespace, [this](model::topic_partition_view tp_p) {
            detach_partition(
              model::ntp(model::kafka_namespace, tp_p.topic, tp_p.partition));
        });

    ssx::spawn_with_gate(_gate, [this] { return reconciliation_loop(); });

    co_return;
}

ss::future<> reconciler::stop() {
    _partition_manager->unregister_manage_notification(_manage_notify_handle);

    _partition_manager->unregister_unmanage_notification(
      _unmanage_notify_handle);

    _as.request_abort();
    _control_sem.broken();

    co_await _gate.close();
}

void reconciler::attach_partition(
  ss::lw_shared_ptr<cluster::partition> partition) {
    if (!is_cloud_partition(partition)) {
        return;
    }
    const auto& ntp = partition->ntp();
    auto tidp = ntp_to_topic_id_partition(ntp);
    if (!tidp.has_value()) {
        vlog(
          lg.error,
          "Cloud topic partition {} does not have a topic id: skipping",
          ntp);
        return;
    }
    vlog(lg.debug, "Attaching partition {} (tidp: {})", ntp, tidp.value());
    auto attached = ss::make_lw_shared<attached_partition_info>(
      tidp.value(), partition);
    auto res = _partitions.try_emplace(ntp, std::move(attached));
    vassert(res.second, "Double registration of ntp {}", ntp);
}

void reconciler::detach_partition(const model::ntp& ntp) {
    if (auto it = _partitions.find(ntp); it != _partitions.end()) {
        vlog(lg.debug, "Detaching partition {}", ntp);
        /*
         * This upcall doesn't synchronize with the rest of the reconciler,
         * which means that once a reference to an attached partition is held,
         * it shouldn't be assumed that the attached partition remains in the
         * _partitions collection.
         */
        _partitions.erase(it);
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
        } catch (const ss::semaphore_timed_out&) {
            // Time to do some work.
        }

        vlog(
          lg.debug,
          "Reconciliation loop tick with {} attached partitions",
          _partitions.size());

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

chunked_vector<reconciler::attached_partition>
reconciler::collect_leader_partitions() const {
    chunked_vector<attached_partition> leaders;
    for (const auto& p : _partitions) {
        if (p.second->partition->is_leader()) {
            leaders.push_back(p.second);
        }
    }

    if (!leaders.empty()) {
        std::shuffle(
          leaders.begin(), leaders.end(), random_generators::internal::gen);
    }
    return leaders;
}

ss::future<> reconciler::reconcile() {
    auto partitions = collect_leader_partitions();
    if (partitions.empty()) {
        vlog(lg.debug, "No leader partitions to reconcile");
        co_return;
    }

    // Begin by creating the set of objects to be built.
    auto metadata_builder = _metastore->object_builder();
    chunked_hash_map<l1::object_id, chunked_vector<attached_partition>>
      oid_to_partitions;
    for (const auto& p : partitions) {
        auto oid = metadata_builder->get_or_create_object_for(p->tidp);
        oid_to_partitions[oid].push_back(p);
    }

    // Process partitions by their object. This should be easier to
    // improve than processing partition-by-partition.
    chunked_vector<built_object_metadata> successful_objects;
    chunked_vector<l1::object_id> failed_objects;
    for (const auto& [oid, partitions] : oid_to_partitions) {
        auto object_fut = co_await ss::coroutine::as_future(
          reconcile_partitions(oid, partitions));
        if (object_fut.failed()) {
            auto ex = object_fut.get_exception();
            const auto is_shutdown = ssx::is_shutdown_exception(ex);
            vlogl(
              lg,
              is_shutdown ? ss::log_level::debug : ss::log_level::error,
              "Exception reconciling {} partitions into object {}: {}",
              partitions.size(),
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
            // Error was already logged in reconcile_partitions.
            continue; // Skip this object and move to the next
        }

        auto obj_metadata = std::move(result).value();
        auto add_result = co_await add_object_metadata(
          oid, obj_metadata, metadata_builder.get());
        if (!add_result.has_value()) {
            failed_objects.push_back(oid);
            // Error was already logged in add_object_metadata.
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
        vlog(
          lg.error,
          "Abandoning reconciliation loop because the L1 metastore operation "
          "failed");
        co_return;
    }
}

ss::future<std::expected<reconciler::built_object_metadata, reconcile_error>>
reconciler::reconcile_partitions(
  const l1::object_id& oid,
  const chunked_vector<attached_partition>& partitions) {
    auto ctx_result = co_await make_context();
    if (!ctx_result.has_value()) {
        co_return std::unexpected(ctx_result.error());
    }
    auto ctx = std::move(ctx_result.value());

    auto fut = co_await ss::coroutine::as_future(
      build_and_put_object(oid, ctx, partitions));

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
        vlogl(
          lg,
          ssx::is_shutdown_exception(ex) ? ss::log_level::debug
                                         : ss::log_level::error,
          "Exception building and putting object {}: {}",
          oid,
          ex);
        co_return std::unexpected(reconcile_error::build_or_put_failure);
    }

    co_return fut.get();
}

ss::future<std::expected<reconciler::built_object_metadata, reconcile_error>>
reconciler::build_and_put_object(
  const l1::object_id& oid,
  builder_context& ctx,
  const chunked_vector<attached_partition>& partitions) {
    // Build the object.
    auto build_result = co_await build_object(ctx, partitions);
    if (!build_result.has_value()) {
        co_return std::unexpected(build_result.error());
    }

    auto obj_meta = std::move(build_result.value());
    if (obj_meta.partitions.empty()) {
        vlog(lg.debug, "Skipping put for object {}: no data", oid);
        co_return std::unexpected(reconcile_error::build_or_put_failure);
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
        vlog(
          lg.warn,
          "Failed to create staging file: {}",
          static_cast<int>(staging_result.error()));
        co_return std::unexpected(reconcile_error::build_or_put_failure);
    }
    ctx.staging = std::move(staging_result).value();

    // Create output stream and builder.
    auto stream_fut = co_await ss::coroutine::as_future(
      ctx.staging->output_stream());
    if (stream_fut.failed()) {
        auto ex = stream_fut.get_exception();
        vlogl(
          lg,
          ssx::is_shutdown_exception(ex) ? ss::log_level::debug
                                         : ss::log_level::error,
          "Failed to create output stream: {}",
          ex);
        co_await ctx.cleanup_staging();
        co_return std::unexpected(reconcile_error::build_or_put_failure);
    }

    auto output_stream = stream_fut.get();
    ctx.builder = l1::object_builder::create(
      std::move(output_stream), l1::object_builder::options{});

    co_return ctx;
}

ss::future<std::expected<reconciler::built_object_metadata, reconcile_error>>
reconciler::build_object(
  builder_context& ctx, const chunked_vector<attached_partition>& partitions) {
    chunked_vector<partition_commit_info> metas;
    metas.reserve(partitions.size());
    for (const auto& partition : partitions) {
        vlog(
          lg.debug,
          "Processing partition {} with LRO {}",
          partition->tidp,
          partition->lro);
        auto meta = co_await add_partition_to_object(ctx, partition);
        if (meta.has_value()) {
            metas.emplace_back(partition, std::move(meta).value());
        }
    }
    metas.shrink_to_fit();

    auto obj_info = co_await ctx.builder->finish();
    vlog(
      lg.debug,
      "Built L1 object from {} partitions ({} partitions didn't fit)",
      metas.size(),
      partitions.size() - metas.size());
    co_return built_object_metadata{
      .object_info = std::move(obj_info), .partitions = std::move(metas)};
}

ss::future<std::expected<void, reconcile_error>>
reconciler::put_object(const l1::object_id& oid, builder_context& ctx) {
    auto put_result = co_await _l1_io->put_object(oid, ctx.staging.get(), &_as);
    if (!put_result.has_value()) {
        vlog(
          lg.error,
          "Failed to put L1 object {}: {}",
          oid,
          static_cast<int>(put_result.error()));
        co_return std::unexpected(reconcile_error::build_or_put_failure);
    }
    vlog(lg.debug, "Successfully put L1 object {}", oid);
    co_return std::expected<void, reconcile_error>{};
}

ss::future<std::optional<partition_metadata>>
reconciler::add_partition_to_object(
  builder_context& ctx, const attached_partition& partition) {
    vlog(
      lg.debug,
      "Processing partition {} with LRO {}",
      partition->tidp,
      partition->lro);

    frontend fe(partition->partition, _data_plane);
    auto reader = co_await make_reader(&fe, partition->lro, ctx.size_budget);
    reconciliation_consumer consumer(ctx.builder.get(), partition->tidp);
    auto metadata = co_await std::move(reader).consume(
      std::move(consumer), model::no_timeout);

    if (!metadata.has_value()) {
        vlog(lg.debug, "No batches found for partition {}", partition->tidp);
        co_return std::nullopt;
    }

    vlog(
      lg.debug,
      "Adding partition {} to L1 object with offsets {}~{}",
      partition->tidp,
      metadata->base_offset,
      metadata->last_offset);

    co_return metadata.value();
}

ss::future<std::expected<void, reconcile_error>>
reconciler::add_object_metadata(
  const l1::object_id& oid,
  const built_object_metadata& obj_meta,
  l1::metastore::object_metadata_builder* meta_builder) {
    // Add metadata for this object to the metadata builder.
    // Remember that there are two kinds of partitions here: the
    // partition of the cloud topic and the partition of the L1 object.
    // There may be multiple partitions in the L1 object for a cloud
    // topic partition.
    for (const auto& partition : obj_meta.partitions) {
        auto [first, last] = obj_meta.object_info.index.partitions.equal_range(
          partition.partition->tidp);
        for (auto it = first; it != last; ++it) {
            const auto& obj_partition = it->second;
            auto add_result = meta_builder->add(
              oid,
              l1::metastore::object_metadata::ntp_metadata{
                .tidp = partition.partition->tidp,
                .base_offset = obj_partition.first_offset,
                .last_offset = obj_partition.last_offset,
                .max_timestamp = obj_partition.max_timestamp,
                .pos = obj_partition.file_position,
                .size = obj_partition.length});
            if (!add_result.has_value()) {
                vlog(
                  lg.error,
                  "Failed to finish metadata for partition {} of object {}: {}",
                  partition.partition->tidp,
                  oid,
                  add_result.error());
                // TODO: The object has been uploaded. The reconciler could
                //       attempt cleanup (or notify a cleanup subsystem).
                co_return std::unexpected(reconcile_error::metadata_failure);
            }
        }
    }

    auto meta_result = meta_builder->finish(
      oid, obj_meta.object_info.footer_offset, obj_meta.object_info.size_bytes);
    if (!meta_result.has_value()) {
        vlog(
          lg.error,
          "Failed to finish metadata for object {}: {}",
          oid,
          meta_result.error());
        // TODO: The object has been uploaded. The reconciler could
        //       attempt cleanup (or notify a cleanup subsystem).
        co_return std::unexpected(reconcile_error::metadata_failure);
    }

    co_return std::expected<void, reconcile_error>{};
}

ss::future<std::expected<void, reconcile_error>> reconciler::commit_objects(
  const chunked_vector<built_object_metadata>& objects,
  std::unique_ptr<l1::metastore::object_metadata_builder> meta_builder) {
    // It's possible to build the terms map as we build the objects, but
    // I think re-iterating over all the object partitions here is worth
    // it in exchange for less context passing among functions.
    l1::metastore::term_offset_map_t terms;
    for (const auto& obj_meta : objects) {
        for (const auto& partition : obj_meta.partitions) {
            chunked_vector<l1::metastore::term_offset> term_offsets;
            for (const auto& [term, first_offset] : partition.metadata.terms) {
                term_offsets.emplace_back(term, first_offset);
            }
            terms[partition.partition->tidp] = std::move(term_offsets);
        }
    }

    auto add_objects_result = co_await _metastore->add_objects(
      std::move(meta_builder), terms);
    if (!add_objects_result.has_value()) {
        vlog(
          lg.error,
          "Failed to add objects to the L1 metastore: {}",
          add_objects_result.error());
        // TODO: The objects have been uploaded. The reconciler could
        //       attempt cleanup (or notify a cleanup subsystem).
        co_return std::unexpected(reconcile_error::metadata_failure);
    }

    // Now update the LRO, taking into account any corrections from
    // the metastore.
    // TODO: Inform L0 of the update.
    const auto& corrected_next_offsets
      = add_objects_result.value().corrected_next_offsets;
    for (const auto& obj_meta : objects) {
        for (const auto& partition : obj_meta.partitions) {
            auto next_lro = kafka::offset::min();
            if (corrected_next_offsets.contains(partition.partition->tidp)) {
                next_lro = corrected_next_offsets.at(partition.partition->tidp);
            } else {
                auto [first, last] = obj_meta.object_info.index.partitions
                                       .equal_range(partition.partition->tidp);
                for (auto it = first; it != last; ++it) {
                    const auto& obj_partition = it->second;
                    next_lro = std::max(
                      next_lro, kafka::next_offset(obj_partition.last_offset));
                }
            }
            partition.partition->lro = next_lro;
        }
    }

    co_return std::expected<void, reconcile_error>{};
}

ss::future<model::record_batch_reader> reconciler::make_reader(
  frontend* fe, kafka::offset start_offset, size_t max_bytes) {
    auto effective_start = co_await fe->sync_effective_start(5s);
    if (!effective_start.has_value()) {
        vlog(
          lg.warn,
          "Error querying partition start offset ({}): {}",
          fe->ntp(),
          effective_start.error());
        co_return model::make_empty_record_batch_reader();
    }

    start_offset = std::max(effective_start.value(), start_offset);

    auto maybe_lso = fe->last_stable_offset();
    if (!maybe_lso.has_value()) {
        vlog(
          lg.warn,
          "Error querying partition LSO ({}): {}",
          fe->ntp(),
          maybe_lso.error());
        co_return model::make_empty_record_batch_reader();
    }

    // It's possible for LSO to be 0, which in this case the previous offset
    // is model::offset::min(), this is the same as the kafka fetch path.
    auto max_offset = kafka::prev_offset(maybe_lso.value());

    if (max_offset < start_offset) {
        co_return model::make_empty_record_batch_reader();
    }

    auto reader = co_await fe->make_reader(
      cloud_topic_log_reader_config(
        start_offset,
        max_offset,
        0,
        max_bytes,
        std::nullopt,
        std::nullopt,
        _as),
      /*debounce_deadline=*/std::nullopt);

    auto tracker = std::make_unique<aborted_transaction_tracker_impl>(
      fe, std::move(reader.ot_state));

    co_return model::make_record_batch_reader<kafka::read_committed_reader>(
      std::move(tracker), std::move(reader.reader));
}

} // namespace cloud_topics::reconciler
