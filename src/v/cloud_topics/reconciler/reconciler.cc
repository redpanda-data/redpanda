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
#include "cloud_topics/reconciler/reconciliation_consumer.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition.h"
#include "kafka/utils/txn_reader.h"
#include "model/namespace.h"
#include "random/generators.h"

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
  cluster::metadata_cache* metadata_cache)
  : _partition_manager(pm)
  , _data_plane(data_plane)
  , _l1_io(l1_io)
  , _metadata_cache(metadata_cache) {}

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
    vlog(lg.debug, "Attaching partition {}", ntp);
    auto attached = ss::make_lw_shared<attached_partition_info>(partition);
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
    auto staging_file_fut = co_await ss::coroutine::as_future(
      _l1_io->create_tmp_file());
    if (staging_file_fut.failed()) {
        auto ex = staging_file_fut.get_exception();
        vlog(lg.error, "Exception creating staging file: {}", ex);
    }
    auto staging_file_result = staging_file_fut.get();
    if (!staging_file_result.has_value()) {
        vlog(
          lg.warn,
          "Failed to create staging file: {}",
          static_cast<int>(staging_file_result.error()));
        co_return;
    }

    auto staging_file = std::move(staging_file_result.value());
    auto output_stream = co_await staging_file->output_stream();

    auto builder = l1::object_builder::create(
      std::move(output_stream), l1::object_builder::options{});

    auto object_fut = co_await ss::coroutine::as_future(
      build_object(builder.get(), staging_file.get()));
    co_await builder->close(); // Always.
    if (object_fut.failed()) {
        auto ex = object_fut.get_exception();
        vlog(lg.error, "Exception building object: {}", ex);
        co_await staging_file->remove();
        co_return;
    }
    auto object = object_fut.get();
    if (!object.has_value()) {
        vlog(lg.debug, "No object to upload, skipping");
        co_return;
    }

    vlog(
      lg.debug,
      "Built L1 object from {} partitions",
      object->partitions.size());

    // Upload object.
    // TODO: The metastore provides the object id once it's
    // used to commit changes to L1.
    auto object_id = l1::create_object_id();
    auto upload_fut = co_await ss::coroutine::as_future(
      _l1_io->put_object(object_id, staging_file.get(), &_as));
    co_await staging_file->remove(); // Always.
    if (upload_fut.failed()) {
        auto ex = upload_fut.get_exception();
        vlog(lg.error, "Exception uploading L1 object {}: {}", object_id, ex);
        co_return;
    }
    auto upload_result = upload_fut.get();
    if (!upload_result.has_value()) {
        vlog(
          lg.warn,
          "Failed to upload L1 object: {}",
          static_cast<int>(upload_result.error()));
        co_return;
    }
    vlog(lg.debug, "Successfully uploaded L1 object {}", object_id);

    // Commit object.
    // TODO: This looks very different with the metastore.
    for (const auto& partition_info : object->partitions) {
        co_await commit_object(object_id, partition_info);
    }
}

ss::future<std::optional<reconciler::built_object>> reconciler::build_object(
  l1::object_builder* builder, l1::staging_file* staging_file) {
    // Copy the leader partition information in case of
    // mid-reconciliation unregistration.
    std::vector<attached_partition> partitions;
    for (const auto& p : _partitions) {
        if (p.second->partition->is_leader()) {
            partitions.push_back(p.second);
        }
    }

    if (partitions.empty()) {
        vlog(lg.debug, "No leader partitions to reconcile");
        co_return std::nullopt;
    }

    // Shuffle to avoid starving partitions.
    // TODO: Investigate how to divide work between partitions with
    // different throughput.
    std::shuffle(
      partitions.begin(), partitions.end(), random_generators::internal::gen);

    built_object result;
    auto size_budget = max_object_size;
    for (const auto& partition : partitions) {
        vlog(
          lg.debug,
          "Processing partition {} with LRO {}",
          partition->partition->ntp(),
          partition->lro);

        frontend fe(partition->partition, _data_plane);
        auto reader = co_await make_reader(&fe, partition->lro, size_budget);
        auto tidp = ntp_to_topic_id_partition(partition->partition->ntp());
        if (!tidp.has_value()) {
            vlog(
              lg.error,
              "Failed to get topic_id for cloud topic partition {}, skipping",
              partition->partition->ntp());
            continue;
        }
        reconciliation_consumer consumer(builder, *tidp);
        auto metadata = co_await std::move(reader).consume(
          std::move(consumer), model::no_timeout);

        if (!metadata.has_value()) {
            vlog(
              lg.debug,
              "No batches found for partition {}",
              partition->partition->ntp());
            continue;
        }

        vlog(
          lg.debug,
          "Adding partition {} to L1 object with offsets {}-{}",
          partition->partition->ntp(),
          metadata->base_offset,
          metadata->last_offset);
        result.partitions.emplace_back(partition, std::move(metadata.value()));

        auto current_size = co_await staging_file->size();
        if (current_size >= max_object_size) {
            break;
        }
        size_budget = max_object_size - current_size;
    }

    if (result.partitions.empty()) {
        co_return std::nullopt;
    }

    result.object_info = co_await builder->finish();
    co_return result;
}

ss::future<> reconciler::commit_object(
  const l1::object_id& object_id, const partition_commit_info& partition_info) {
    /*
     * TODO register the L1 object with L1 metastore using object_id and
     * partition_info.
     */
    const auto& part = partition_info.partition->partition;
    const auto& metadata = partition_info.metadata;

    partition_info.partition->lro = metadata.last_offset + kafka::offset(1);

    vlog(
      lg.debug,
      "Committed overlay to object {} for {} log {}~{}. New LRO {}",
      object_id,
      part->ntp(),
      metadata.base_offset,
      metadata.last_offset,
      partition_info.partition->lro);

    co_return;
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
