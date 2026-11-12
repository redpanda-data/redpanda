/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/archival/staging_uploader.h"

#include "base/vlog.h"
#include "bytes/iostream.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/types.h"
#include "cluster/archival/async_data_uploader.h"
#include "cluster/archival/logger.h"
#include "cluster/partition_manager.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "metrics/prometheus_sanitize.h"
#include "ssx/future-util.h"
#include "ssx/sformat.h"
#include "storage/api.h"
#include "storage/staging_floor.h"
#include "utils/uuid.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/maybe_yield.hh>

using namespace std::chrono_literals;

namespace archival {

namespace {

constexpr size_t read_buffer_size = 128_KiB;

} // namespace

staging_uploader::staging_uploader(
  ss::sharded<cloud_storage::remote>& remote,
  ss::sharded<cluster::partition_manager>& pm,
  ss::sharded<storage::api>& storage,
  cloud_storage_clients::bucket_name primary_bucket)
  : _remote(remote)
  , _storage(storage)
  , _pm(pm)
  , _primary_bucket(std::move(primary_bucket))
  , _boot_id(ss::sstring(uuid_t::create()).substr(0, 8))
  , _rtc(_as) {}

ss::sstring staging_uploader::cluster_label() const {
    auto uuid = _storage.local().get_cluster_uuid();
    return uuid.has_value() ? ssx::sformat("{}", uuid.value())
                            : ss::sstring{"none"};
}

ss::future<> staging_uploader::start() {
    setup_metrics();
    ssx::spawn_with_gate(_gate, [this] { return run_loop(); });
    co_return;
}

ss::future<> staging_uploader::maybe_gc() {
    auto& cfg = config::shard_local_cfg();
    auto retention = cfg.tiered_storage_staging_retention_ms();
    if (retention <= 0ms) {
        co_return;
    }
    auto interval = std::chrono::duration_cast<ss::lowres_clock::duration>(
      retention / 4);
    if (ss::lowres_clock::now() - _last_gc < interval) {
        co_return;
    }
    _last_gc = ss::lowres_clock::now();
    // Staging objects are transient: once their extents are covered by the
    // canonical tier (archiver/reconciler progress) an object is redundant.
    // Age is a safe, cheap proxy as long as retention stays comfortably above
    // the worst-case tiering lag. Delete only THIS shard's own prefix.
    auto prefix = ssx::sformat(
      "staging/{}/{}/{}/",
      cluster_label(),
      config::node().node_id().value_or(model::node_id{-1})(),
      ss::this_shard_id());
    retry_chain_node fib(60s, 200ms, &_rtc);
    std::optional<ss::sstring> continuation;
    size_t deleted = 0;
    const auto cutoff
      = std::chrono::system_clock::now()
        - std::chrono::duration_cast<std::chrono::system_clock::duration>(
          retention);
    while (!_as.abort_requested()) {
        auto list = co_await _remote.local().list_objects(
          _primary_bucket,
          fib,
          cloud_storage_clients::object_key{prefix},
          std::nullopt,
          std::nullopt,
          std::nullopt,
          continuation);
        if (list.has_error()) {
            co_return;
        }
        std::vector<cloud_storage_clients::object_key> stale;
        for (const auto& item : list.value().contents) {
            if (item.last_modified <= cutoff) {
                stale.emplace_back(item.key);
            }
        }
        if (!stale.empty()) {
            auto n = stale.size();
            auto res = co_await _remote.local().delete_objects(
              _primary_bucket, std::move(stale), fib);
            if (res == cloud_storage::upload_result::success) {
                deleted += n;
            }
        }
        if (!list.value().is_truncated) {
            break;
        }
        continuation = list.value().next_continuation_token;
    }
    if (deleted > 0) {
        vlog(
          archival_log.debug,
          "staging GC: reclaimed {} object(s) older than {}ms under {}",
          deleted,
          retention.count(),
          prefix);
    }
}

ss::future<> staging_uploader::stop() {
    _as.request_abort();
    co_await _gate.close();
    // Release eviction holds owned by this shard's uploader.
    storage::staging_floor::instance().clear();
}

iobuf staging_uploader::make_object(iobuf data, staging_index index) {
    auto index_buf = serde::to_iobuf(std::move(index));
    uint64_t index_size = index_buf.size_bytes();
    data.append(std::move(index_buf));
    data.append(reinterpret_cast<const char*>(&index_size), sizeof(index_size));
    uint32_t version = footer_version;
    data.append(reinterpret_cast<const char*>(&version), sizeof(version));
    uint32_t magic = footer_magic;
    data.append(reinterpret_cast<const char*>(&magic), sizeof(magic));
    return data;
}

ss::future<> staging_uploader::run_loop() {
    while (!_as.abort_requested()) {
        auto& cfg = config::shard_local_cfg();
        if (!cfg.tiered_storage_staging_enabled()) {
            co_await ss::sleep_abortable(1s, _as).handle_exception_type(
              [](const ss::sleep_aborted&) {});
            continue;
        }
        bool backlog = false;
        try {
            backlog = co_await round();
            co_await maybe_gc();
            auto reconcile_interval
              = cfg.tiered_storage_staging_reconcile_interval_ms();
            if (
              reconcile_interval > 0ms
              && ss::lowres_clock::now() - _last_reconcile
                   > reconcile_interval) {
                _last_reconcile = ss::lowres_clock::now();
                co_await reconcile_secondary();
            }
        } catch (...) {
            auto eptr = std::current_exception();
            if (ssx::is_shutdown_exception(eptr)) {
                break;
            }
            vlog(archival_log.warn, "staging_uploader: round failed: {}", eptr);
        }
        if (!backlog) {
            auto interval = cfg.tiered_storage_staging_upload_interval_ms();
            co_await ss::sleep_abortable(interval, _as)
              .handle_exception_type([](const ss::sleep_aborted&) {});
        } else {
            co_await ss::coroutine::maybe_yield();
        }
    }
}

ss::future<bool> staging_uploader::round() {
    auto& cfg = config::shard_local_cfg();
    const size_t budget = cfg.tiered_storage_staging_max_bytes_per_round();

    iobuf data;
    staging_index index;
    size_t used = 0;
    bool deferred = false;
    int64_t lag = 0;

    // Snapshot the partition set: the table can mutate across scheduling
    // points while we read from individual partitions.
    std::vector<ss::lw_shared_ptr<cluster::partition>> parts;
    for (const auto& [ntp, part] : _pm.local().partitions()) {
        if (ntp.ns != model::kafka_namespace) {
            continue;
        }
        if (!part->is_elected_leader()) {
            continue;
        }
        parts.push_back(part);
    }

    for (const auto& part : parts) {
        if (_as.abort_requested()) {
            break;
        }
        const auto& ntp = part->ntp();
        auto committed = part->committed_offset();
        auto it = _cursors.find(ntp);
        if (it == _cursors.end()) {
            auto start = part->raft_start_offset();
            it = _cursors.emplace(ntp, model::prev_offset(start)).first;
            storage::staging_floor::instance().set(ntp, it->second);
        }
        auto base = model::next_offset(it->second);
        if (committed < base) {
            continue;
        }
        if (deferred || (used >= budget && used > 0)) {
            // budget exhausted this round; count remaining backlog as lag
            lag += committed() - base() + 1;
            deferred = true;
            continue;
        }
        auto upl_result = co_await segment_upload::make_segment_upload(
          part.get(),
          inclusive_offset_range(base, committed),
          read_buffer_size,
          ss::current_scheduling_group(),
          model::timeout_clock::now() + 10s);
        if (upl_result.has_error()) {
            // Typically the range was trimmed locally before staging could
            // read it (possible when local retention undercuts the staging
            // cursor). Skip forward: the canonical tier covers this range.
            vlog(
              archival_log.warn,
              "staging_uploader: {} range [{},{}] unreadable ({}), advancing "
              "cursor",
              ntp,
              base,
              committed,
              upl_result.error().message());
            it->second = committed;
            storage::staging_floor::instance().set(ntp, it->second);
            ++_staging_gaps;
            continue;
        }
        auto upl = std::move(upl_result.value());
        auto size = upl->get_size_bytes();
        if (used > 0 && used + size > budget) {
            co_await upl->close();
            lag += committed() - base() + 1;
            deferred = true;
            continue;
        }
        auto meta = upl->get_meta();
        auto stream = co_await std::move(*upl).detach_stream();
        auto buf = co_await read_iobuf_exactly(stream, size);
        co_await stream.close();
        if (buf.size_bytes() != size) {
            vlog(
              archival_log.warn,
              "staging_uploader: {} short read {} != {}, skipping round entry",
              ntp,
              buf.size_bytes(),
              size);
            continue;
        }
        staging_extent extent;
        extent.ntp = ntp;
        extent.term = part->term();
        extent.base = meta.offsets.base;
        extent.last = meta.offsets.last;
        extent.byte_offset = used;
        extent.byte_len = size;
        extent.compacted = meta.is_compacted;
        // The revision recovery will compare against: recovered/read-replica
        // topics carry the original topic's revision in remote_rev; regular
        // topics use their create (topic) revision — which is exactly what a
        // future whole-cluster restore sets as remote_revision.
        {
            auto remote_rev = part->get_ntp_config().get_remote_revision();
            extent.revision
              = remote_rev != model::initial_revision_id{0}
                  ? remote_rev
                  : model::initial_revision_id{
                      part->get_ntp_config().get_topic_revision()()};
        }
        index.extents.push_back(std::move(extent));
        used += size;
        data.append(std::move(buf));
        it->second = meta.offsets.last;
        storage::staging_floor::instance().set(ntp, it->second);
    }

    _lag_offsets = lag;
    if (index.extents.empty()) {
        co_return deferred;
    }

    auto object = make_object(std::move(data), index);
    auto key = ssx::sformat(
      "staging/{}/{}/{}/{}-{:016x}.sto",
      cluster_label(),
      config::node().node_id().value_or(model::node_id{-1})(),
      ss::this_shard_id(),
      _boot_id,
      _seq++);

    auto secondary = cfg.tiered_storage_staging_secondary_bucket();

    auto primary_fut = put(
      _primary_bucket, key, object.share(0, object.size_bytes()));
    if (secondary.has_value()) {
        auto sec_bucket = cloud_storage_clients::bucket_name{*secondary};
        auto sec_payload = object.share(0, object.size_bytes());
        auto sec_res = co_await ss::coroutine::as_future(
          put(sec_bucket, key, std::move(sec_payload)));
        if (
          sec_res.failed()
          || sec_res.get() != cloud_storage::upload_result::success) {
            ++_upload_errors_secondary;
            if (
              _secondary_queue.size() < max_secondary_queue_objects
              && _secondary_queue_bytes + object.size_bytes()
                   <= max_secondary_queue_bytes) {
                _secondary_queue_bytes += object.size_bytes();
                _secondary_queue.push_back(
                  pending_secondary{
                    .key = key,
                    .payload = object.share(0, object.size_bytes())});
            } else {
                ++_secondary_dropped;
            }
        }
    }

    auto res = co_await ss::coroutine::as_future(std::move(primary_fut));
    if (res.failed() || res.get() != cloud_storage::upload_result::success) {
        ++_upload_errors_primary;
        // do NOT advance: rewind cursors for this round's extents so the data
        // is re-staged next round.
        for (const auto& e : index.extents) {
            auto it = _cursors.find(e.ntp);
            if (it != _cursors.end() && it->second == e.last) {
                it->second = model::prev_offset(e.base);
                storage::staging_floor::instance().set(e.ntp, it->second);
            }
        }
        co_return deferred;
    }

    ++_uploads_total;
    _staged_bytes_total += used;
    vlog(
      archival_log.debug,
      "staging_uploader: uploaded {} ({} extents, {} bytes)",
      key,
      index.extents.size(),
      used);

    if (secondary.has_value() && !_secondary_queue.empty()) {
        co_await drain_secondary_queue();
    }
    co_return deferred;
}

ss::future<cloud_storage::upload_result> staging_uploader::put(
  cloud_storage_clients::bucket_name bucket, ss::sstring key, iobuf buf) {
    retry_chain_node fib(30s, 100ms, &_rtc);
    co_return co_await _remote.local().upload_object(
      cloud_storage::upload_request{
        .transfer_details = cloud_io::transfer_details{
          .bucket = std::move(bucket),
          .key = cloud_storage_clients::object_key(std::move(key)),
          .parent_rtc = fib,
        },
        .type = cloud_storage::upload_type::object,
        .payload = std::move(buf),
      });
}

ss::future<> staging_uploader::drain_secondary_queue() {
    auto& cfg = config::shard_local_cfg();
    auto secondary = cfg.tiered_storage_staging_secondary_bucket();
    if (!secondary.has_value()) {
        _secondary_queue.clear();
        _secondary_queue_bytes = 0;
        co_return;
    }
    auto bucket = cloud_storage_clients::bucket_name{*secondary};
    while (!_secondary_queue.empty() && !_as.abort_requested()) {
        auto& head = _secondary_queue.front();
        auto payload = head.payload.share(0, head.payload.size_bytes());
        auto res = co_await ss::coroutine::as_future(
          put(bucket, head.key, std::move(payload)));
        if (
          res.failed() || res.get() != cloud_storage::upload_result::success) {
            // still unavailable; retry on a later round
            co_return;
        }
        _secondary_queue_bytes -= head.payload.size_bytes();
        _secondary_queue.pop_front();
    }
}

ss::future<> staging_uploader::reconcile_secondary() {
    auto& cfg = config::shard_local_cfg();
    auto secondary = cfg.tiered_storage_staging_secondary_bucket();
    if (!secondary.has_value()) {
        co_return;
    }
    auto sec_bucket = cloud_storage_clients::bucket_name{*secondary};
    auto prefix = ssx::sformat(
      "staging/{}/{}/{}/",
      cluster_label(),
      config::node().node_id().value_or(model::node_id{-1})(),
      ss::this_shard_id());

    auto list_keys = [this, &prefix](cloud_storage_clients::bucket_name bucket)
      -> ss::future<std::optional<absl::node_hash_map<ss::sstring, size_t>>> {
        absl::node_hash_map<ss::sstring, size_t> keys;
        std::optional<ss::sstring> continuation;
        while (true) {
            retry_chain_node fib(30s, 100ms, &_rtc);
            auto res = co_await _remote.local().list_objects(
              bucket,
              fib,
              cloud_storage_clients::object_key{prefix},
              std::nullopt,
              std::nullopt,
              std::nullopt,
              continuation);
            if (res.has_error()) {
                co_return std::nullopt;
            }
            for (const auto& item : res.value().contents) {
                keys.emplace(item.key, item.size_bytes);
            }
            if (!res.value().is_truncated) {
                break;
            }
            continuation = res.value().next_continuation_token;
        }
        co_return keys;
    };

    auto primary_keys = co_await list_keys(_primary_bucket);
    if (!primary_keys) {
        co_return;
    }
    auto secondary_keys = co_await list_keys(sec_bucket);
    if (!secondary_keys) {
        co_return;
    }
    for (const auto& [key, size] : *primary_keys) {
        if (_as.abort_requested()) {
            co_return;
        }
        auto it = secondary_keys->find(key);
        if (it != secondary_keys->end() && it->second == size) {
            continue;
        }
        iobuf payload;
        retry_chain_node fib(30s, 100ms, &_rtc);
        auto dl = co_await _remote.local().download_object(
          cloud_storage::download_request{
            .transfer_details = cloud_io::transfer_details{
              .bucket = _primary_bucket,
              .key = cloud_storage_clients::object_key(key),
              .parent_rtc = fib,
            },
            .type = cloud_storage::download_type::object,
            .payload = payload,
          });
        if (dl != cloud_storage::download_result::success) {
            continue;
        }
        auto up = co_await ss::coroutine::as_future(
          put(sec_bucket, key, std::move(payload)));
        if (!up.failed() && up.get() == cloud_storage::upload_result::success) {
            ++_secondary_reconciled;
            vlog(
              archival_log.info,
              "staging_uploader: reconciled {} to secondary",
              key);
        }
    }
}

void staging_uploader::setup_metrics() {
    namespace sm = ss::metrics;
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }
    _metrics.add_group(
      prometheus_sanitize::metrics_name("tiered_storage:staging"),
      {
        sm::make_counter(
          "uploads_total",
          [this] { return _uploads_total; },
          sm::description("Number of staging objects uploaded")),
        sm::make_counter(
          "staged_bytes_total",
          [this] { return _staged_bytes_total; },
          sm::description("Total bytes staged to object storage")),
        sm::make_counter(
          "upload_errors_primary_total",
          [this] { return _upload_errors_primary; },
          sm::description("Failed staging uploads to the primary bucket")),
        sm::make_counter(
          "upload_errors_secondary_total",
          [this] { return _upload_errors_secondary; },
          sm::description("Failed staging uploads to the secondary bucket")),
        sm::make_counter(
          "secondary_reconciled_total",
          [this] { return _secondary_reconciled; },
          sm::description(
            "Staging objects re-uploaded to the secondary by the "
            "anti-entropy sweep")),
        sm::make_counter(
          "secondary_dropped_total",
          [this] { return _secondary_dropped; },
          sm::description(
            "Staging objects dropped from the secondary retry queue")),
        sm::make_counter(
          "gaps_total",
          [this] { return _staging_gaps; },
          sm::description(
            "Offset ranges skipped because they were locally unreadable")),
        sm::make_gauge(
          "lag_offsets",
          [this] { return _lag_offsets; },
          sm::description(
            "Committed offsets not yet staged (deferred by round budget)")),
        sm::make_gauge(
          "secondary_queue_depth",
          [this] { return static_cast<int64_t>(_secondary_queue.size()); },
          sm::description("Staging objects awaiting secondary re-upload")),
      });
}

} // namespace archival
