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
#include "cloud_topics/reconciler/reconciler.h"

#include "base/vlog.h"
#include "cloud_storage/configuration.h"
#include "cluster/partition.h"
#include "kafka/server/partition_proxy.h"
#include "kafka/utils/txn_reader.h"
#include "model/namespace.h"
#include "random/generators.h"

#include <seastar/core/shared_ptr.hh>
#include <seastar/util/log.hh>

namespace {
ss::logger lg("reconciler");

/*
 * Temporary hack for identifying cloud partitions (topic has "_ct" suffix).
 * This can be removed once we teach Redpanda about this new type of topic.
 */
bool is_cloud_partition(
  const ss::lw_shared_ptr<cluster::partition>& partition) {
    return partition->get_ntp_config().cloud_topic_enabled();
}
} // namespace

namespace experimental::cloud_topics::reconciler {

reconciler::reconciler(
  ss::sharded<cluster::partition_manager>* pm,
  ss::sharded<cloud_io::remote>* cloud_io,
  std::optional<cloud_storage_clients::bucket_name> bucket)
  : _partition_manager(pm)
  , _cloud_io(cloud_io) {
    if (bucket.has_value()) {
        _bucket = std::move(bucket.value());
    } else {
        _bucket = cloud_storage_clients::bucket_name(
          cloud_storage::configuration::get_bucket_config().value().value());
    }
}

ss::future<> reconciler::start() {
    _manage_notify_handle
      = _partition_manager->local().register_manage_notification(
        model::kafka_namespace,
        [this](ss::lw_shared_ptr<cluster::partition> p) {
            attach_partition(std::move(p));
        });

    _unmanage_notify_handle
      = _partition_manager->local().register_unmanage_notification(
        model::kafka_namespace, [this](model::topic_partition_view tp_p) {
            detach_partition(
              model::ntp(model::kafka_namespace, tp_p.topic, tp_p.partition));
        });

    ssx::spawn_with_gate(_gate, [this] { return reconciliation_loop(); });

    co_return;
}

ss::future<> reconciler::stop() {
    _partition_manager->local().unregister_manage_notification(
      _manage_notify_handle);

    _partition_manager->local().unregister_unmanage_notification(
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
    vlog(lg.info, "Reconciler is attaching cloud partition {}", ntp);
    auto attached = ss::make_lw_shared<attached_partition_info>(partition);
    auto res = _partitions.try_emplace(ntp, std::move(attached));
    vassert(res.second, "Double registration of ntp {}", ntp);
}

void reconciler::detach_partition(const model::ntp& ntp) {
    if (auto it = _partitions.find(ntp); it != _partitions.end()) {
        vlog(lg.info, "Reconciler is detatching partition {}", ntp);
        /*
         * This upcall doesn't synchronize with the rest of the reconciler,
         * which means that once a reference to an attached partition is held,
         * it shouldn't be assumed that the attached partition remains in the
         * _partitions collection.
         */
        _partitions.erase(it);
    }
}

void reconciler::object::add(range range, const attached_partition& partition) {
    vassert(!range.data.empty(), "cannot add an empty range to object");

    const auto physical_offset_start = data.size_bytes();
    data.append(std::move(range.data));
    const auto physical_offset_end = data.size_bytes();

    ranges.emplace_back(
      partition, physical_offset_start, physical_offset_end, range.info);
}

ss::future<> reconciler::reconciliation_loop() {
    /*
     * polling is not particularly efficient, and in practice, we'll probably
     * want to look into receiving upcalls from partitions announcing that new
     * data is available.
     */
    constexpr std::chrono::seconds poll_frequency(10);

    while (!_gate.is_closed()) {
        try {
            co_await _control_sem.wait(
              poll_frequency, std::max(_control_sem.current(), size_t(1)));
        } catch (const ss::semaphore_timed_out&) {
            // time to do some work
        }

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
    auto object = co_await build_object();
    if (!object.has_value()) {
        co_return;
    }

    auto result = co_await upload_object(std::move(object->data));
    if (result != cloud_io::upload_result::success) {
        vlog(lg.info, "Failed to upload L1 object: {}", result);
        co_return;
    }

    // commit for each partition represented in the uploaded object
    for (const auto& range : object->ranges) {
        co_await commit_object(range);
    }
}

ss::future<std::optional<reconciler::object>> reconciler::build_object() {
    // light-weight copy for stable iteration
    std::vector<attached_partition> partitions;
    for (const auto& p : _partitions) {
        if (p.second->partition->is_leader()) {
            partitions.push_back(p.second);
        }
    }

    // avoid starving partitions
    std::shuffle(
      partitions.begin(), partitions.end(), random_generators::internal::gen);

    object object;
    auto size_budget = max_object_size;
    for (const auto& partition : partitions) {
        auto reader = co_await make_reader(partition, size_budget);
        auto range = co_await std::move(reader).consume(
          range_batch_consumer{}, model::no_timeout);
        if (range.has_value()) {
            object.add(std::move(*range), partition);
            size_budget -= std::min(object.data.size_bytes(), size_budget);
        }
    }

    if (object.data.empty()) {
        co_return std::nullopt;
    }

    co_return object;
}

ss::future<cloud_io::upload_result> reconciler::upload_object(iobuf payload) {
    const cloud_storage_clients::object_key key(
      fmt::format("l1_{}", uuid_t::create()));

    retry_chain_node rtc(
      _as,
      ss::lowres_clock::now() + std::chrono::seconds(20),
      std::chrono::seconds(1));

    co_return co_await _cloud_io->local().upload_object({
      .transfer_details = {
        .bucket = _bucket,
        .key = key,
        .parent_rtc = rtc,
      },
      .display_str = "l1_object",
      .payload = std::move(payload),
    });
}

ss::future<> reconciler::commit_object(const object_range_info& range) {
    /*
     * TODO: we aren't actually replicating an overlay batch here yet. This will
     * come at the time of integration with the data layout STM.
     */
    range.partition->lro = range.info.last_offset + model::offset(1);

    vlog(
      lg.info,
      "Committed overlay for {} phy {}~{} log {}~{}. New LRO {}",
      range.partition->partition->ntp(),
      range.physical_offset_start,
      range.physical_offset_end,
      range.info.base_offset,
      range.info.last_offset,
      range.partition->lro);

    co_return;
}

ss::future<model::record_batch_reader>
reconciler::make_reader(const attached_partition& partition, size_t max_bytes) {
    auto proxy = kafka::make_partition_proxy(partition->partition);

    auto effective_start = co_await proxy.sync_effective_start();
    if (effective_start.has_error()) {
        vlog(
          lg.info,
          "Error querying partition start offset ({}): {}",
          proxy.ntp(),
          effective_start.error());
        co_return model::make_empty_record_batch_reader();
    }

    model::offset start_offset = std::max(
      effective_start.value(), partition->lro);

    auto maybe_lso = proxy.last_stable_offset();
    if (maybe_lso.has_error()) {
        vlog(
          lg.info,
          "Error querying partition LSO ({}): {}",
          proxy.ntp(),
          maybe_lso.error());
        co_return model::make_empty_record_batch_reader();
    }

    // It's possible for LSO to be 0, which in this case the previous offset
    // is model::offset::min(), this is the same as the kafka fetch path.
    model::offset max_offset = model::prev_offset(maybe_lso.value());

    if (max_offset < start_offset) {
        co_return model::make_empty_record_batch_reader();
    }

    auto reader = co_await proxy.make_reader(storage::log_reader_config(
      start_offset,
      max_offset,
      0,
      max_bytes,
      ss::default_priority_class(),
      std::nullopt,
      std::nullopt,
      _as));

    auto tracker = kafka::aborted_transaction_tracker::create_default(
      &proxy, std::move(reader.ot_state));

    co_return model::make_record_batch_reader<kafka::read_committed_reader>(
      std::move(tracker), std::move(reader.reader));
}

} // namespace experimental::cloud_topics::reconciler
