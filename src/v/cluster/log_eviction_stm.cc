// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/log_eviction_stm.h"

#include "bytes/iostream.h"
#include "cluster/errc.h"
#include "cluster/logger.h"
#include "cluster/prefix_truncate_record.h"
#include "model/fundamental.h"
#include "raft/consensus.h"
#include "raft/fundamental.h"
#include "serde/rw/envelope.h"
#include "serde/rw/iobuf.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/sleep.hh>

namespace cluster {

struct snapshot_data
  : serde::
      envelope<snapshot_data, serde::version<0>, serde::compat_version<0>> {
    model::offset effective_start_offset{};

    auto serde_fields() { return std::tie(effective_start_offset); }

    friend std::ostream& operator<<(std::ostream& os, const snapshot_data& d) {
        fmt::print(
          os, "{{ effective_start_offset: {} }}", d.effective_start_offset);
        return os;
    }
};

log_eviction_stm::log_eviction_stm(
  raft::consensus* raft, ss::logger& logger, storage::kvstore& kvstore)
  : base_t("log_eviction_stm.snapshot", logger, raft, kvstore) {}

ss::future<> log_eviction_stm::start() {
    ssx::spawn_with_gate(_gate, [this] { return monitor_log_eviction(); });
    ssx::spawn_with_gate(
      _gate, [this] { return handle_log_eviction_events(); });
    return base_t::start();
}

ss::future<> log_eviction_stm::stop() {
    _as.request_abort();
    _has_pending_truncation.broken();
    co_await base_t::stop();
}

ss::future<> log_eviction_stm::handle_log_eviction_events() {
    static constexpr auto retry_backoff_time = 5s;
    /// This method is executed as a background fiber and it attempts to write
    /// snapshots as close to effective_start_offset as possible.
    auto gh = _gate.hold();

    bool previous_iter_truncated_everything = true;
    while (!_as.abort_requested() && !_gate.is_closed()) {
        /// This background fiber can be woken-up via apply() when special
        /// batches are processed or by the storage layer when local
        /// eviction is triggered.
        try {
            if (previous_iter_truncated_everything) {
                co_await _has_pending_truncation.wait();
            } else {
                // Previous iter didn't get everything (e.g. because max
                // collectible offset prevented use from truncating). Be sure
                // to try again soon even if no other notifications come in.
                co_await _has_pending_truncation.wait(retry_backoff_time);
            }
        } catch (const ss::condition_variable_timed_out&) {
            /// There is still more data to truncate
        } catch (const ss::abort_requested_exception&) {
            break;
        } catch (const ss::broken_condition_variable&) {
            /// stop() has been called
            break;
        } catch (const ss::broken_semaphore&) {
            /// stop() has been called
            break;
        }

        auto evict_until = std::max(
          _delete_records_eviction_offset, _storage_eviction_offset);
        if (_raft->last_snapshot_index() >= evict_until) {
            previous_iter_truncated_everything = true;
            continue;
        }
        auto truncation_point = _raft->log()->index_lower_bound(evict_until);
        if (!truncation_point) {
            vlog(
              _log.warn,
              "unable to find index lower bound for {}",
              evict_until);
            previous_iter_truncated_everything = false;
            continue;
        }

        vassert(
          truncation_point <= evict_until,
          "Calculated boundary {} must be <= eviction offset {} ",
          truncation_point,
          evict_until);
        try {
            co_await do_write_raft_snapshot(*truncation_point);
            if (_raft->last_snapshot_index() < truncation_point) {
                previous_iter_truncated_everything = false;
                continue;
            }
            // NOTE: the offsets changed such that the truncation point may
            // have changed, it will be caught in the next iteration, since we
            // would have been signaled.
            previous_iter_truncated_everything = true;
        } catch (const ss::abort_requested_exception&) {
            // ignore abort requested exception, shutting down
        } catch (const ss::gate_closed_exception&) {
            // ignore gate closed exception, shutting down
        } catch (const ss::broken_semaphore&) {
        } catch (const std::exception& e) {
            vlog(
              _log.error,
              "Error occurred when attempting to write snapshot: {}",
              e);
        }
    }
}

ss::future<model::offset> log_eviction_stm::storage_eviction_event() {
    return _raft->monitor_log_eviction(_as);
}

ss::future<> log_eviction_stm::monitor_log_eviction() {
    /// This method is executed as a background fiber and is listening for
    /// eviction events from the storage layer. These events will trigger a
    /// write snapshot, and the log will be prefix truncated.
    auto gh = _gate.hold();
    while (!_as.abort_requested()) {
        try {
            auto eviction_offset = co_await storage_eviction_event();
            if (eviction_offset > _storage_eviction_offset) {
                _storage_eviction_offset = eviction_offset;
                _has_pending_truncation.signal();
            }
        } catch (const ss::abort_requested_exception&) {
            // ignore abort requested exception, shutting down
        } catch (const ss::gate_closed_exception&) {
            // ignore gate closed exception, shutting down
        } catch (const std::exception& e) {
            vlog(_log.info, "Error handling log eviction - {}", e);
        }
    }
}

ss::future<>
log_eviction_stm::do_write_raft_snapshot(model::offset truncation_point) {
    vlog(
      _log.trace,
      "requested to write raft snapshot (prefix_truncate) at {}",
      truncation_point);
    if (truncation_point <= _raft->last_snapshot_index()) {
        co_return;
    }
    co_await _raft->visible_offset_monitor().wait(
      truncation_point, model::no_timeout, _as);
    co_await _raft->refresh_commit_index();
    co_await _raft->log()->stm_manager()->ensure_snapshot_exists(
      truncation_point);
    const auto max_collectible_offset
      = _raft->log()->stm_manager()->max_collectible_offset();
    if (truncation_point > max_collectible_offset) {
        truncation_point = max_collectible_offset;
        if (truncation_point <= _raft->last_snapshot_index()) {
            /// Cannot truncate, have already reached maximum allowable
            co_return;
        }
        vlog(
          _log.trace,
          "Can only evict up to offset: {}, asked to evict to: {} ",
          max_collectible_offset,
          truncation_point);
    }
    if (truncation_point <= _raft->last_snapshot_index()) {
        vlog(
          _log.trace,
          "Skipping writing snapshot as Raft already progressed with the new "
          "snapshot. Current raft snapshot index: {}, requested truncation "
          "point: {}",
          _raft->last_snapshot_index(),
          truncation_point);
        co_return;
    }
    vlog(
      _log.debug,
      "Requesting raft snapshot with final offset: {}",
      truncation_point);
    auto snapshot_result = co_await _raft->stm_manager()->take_snapshot(
      truncation_point);
    // we need to check snapshot index again as it may already progressed after
    // snapshot is taken by stm_manager
    if (truncation_point <= _raft->last_snapshot_index()) {
        vlog(
          _log.trace,
          "Skipping writing snapshot as Raft already progressed with the new "
          "snapshot. Current raft snapshot index: {}, requested truncation "
          "point: {}",
          _raft->last_snapshot_index(),
          truncation_point);
        co_return;
    }
    co_await _raft->write_snapshot(raft::write_snapshot_cfg(
      snapshot_result.last_included_offset, std::move(snapshot_result.data)));
}

kafka::offset log_eviction_stm::kafka_start_offset_override() {
    if (_cached_kafka_start_offset_override != kafka::offset{}) {
        return _cached_kafka_start_offset_override;
    }

    // Since the STM doesn't snapshot `_cached_kafka_start_override` its
    // possible for it to be lost during restarts. Therefore the raft offset
    // which is snapshotted will be translated if possible.
    if (_delete_records_eviction_offset == model::offset{}) {
        return kafka::offset{};
    }

    auto raft_start_offset_override = model::next_offset(
      _delete_records_eviction_offset);

    // This handles an edge case where the stm will not record any raft
    // offsets that do not land in local storage. Hence returning
    // `kafka::offset{}` indicates to the caller that the archival stm
    // should be queried for the offset instead.
    if (raft_start_offset_override <= _raft->start_offset()) {
        return kafka::offset{};
    }

    _cached_kafka_start_offset_override = model::offset_cast(
      _raft->log()->from_log_offset(raft_start_offset_override));

    return _cached_kafka_start_offset_override;
}

ss::future<result<kafka::offset, std::error_code>>
log_eviction_stm::sync_kafka_start_offset_override(
  model::timeout_clock::duration timeout) {
    /// Call this method to ensure followers have processed up until the
    /// most recent known version of the special batch. This is particularly
    /// useful to know if the start offset is up to date in the case
    /// leadership has recently changed for example.
    auto term = _raft->term();
    return sync(timeout).then(
      [this, term](bool success) -> result<kafka::offset, std::error_code> {
          if (!success) {
              if (term != _raft->term()) {
                  return errc::not_leader;
              } else {
                  return errc::timeout;
              }
          }
          return kafka_start_offset_override();
      });
}

model::offset log_eviction_stm::effective_start_offset() const {
    /// The start offset is the max of either the last snapshot index or the
    /// most recent delete records eviciton offset. This is because even after
    /// bootstrap the larger of the two will reflect the most recent event that
    /// has occurred, and will be the correct start offset.
    ///
    /// NOTE: Cannot replace last_snapshot_index with _storage_eviction_offset
    /// as this is the requested eviction offset and its not persisted anywhere.
    /// In the event this is set but a crash occurred before write_snapshot was
    /// called (occurs in background) it would appear that the start offset was
    /// incremented then returned to a previous value.
    return model::next_offset(
      std::max(_raft->last_snapshot_index(), _delete_records_eviction_offset));
}

ss::future<log_eviction_stm::offset_result> log_eviction_stm::truncate(
  model::offset rp_start_offset,
  kafka::offset kafka_start_offset,
  ss::lowres_clock::time_point deadline,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    /// Create the special prefix_truncate batch, it is a model::record_batch
    /// with exactly one record within it, the point at which to truncate
    storage::record_batch_builder builder(
      model::record_batch_type::prefix_truncate, model::offset(0));
    /// Everything below the requested offset should be truncated, requested
    /// offset itself will be the new low_watermark (readable)
    prefix_truncate_record val;
    val.rp_start_offset = rp_start_offset;
    val.kafka_start_offset = kafka_start_offset;
    builder.add_raw_kv(
      serde::to_iobuf(prefix_truncate_key), serde::to_iobuf(std::move(val)));
    auto batch = std::move(builder).build();

    /// After command replication all that can be guaranteed is that the command
    /// was replicated
    vlog(
      _log.info,
      "Replicating prefix_truncate command, redpanda start offset: {}, kafka "
      "start offset: {} "
      "current last snapshot offset: {}, current last visible offset: {}",
      val.rp_start_offset,
      val.kafka_start_offset,
      _raft->last_snapshot_index(),
      _raft->last_visible_index());

    auto res = co_await replicate_command(std::move(batch), deadline, as);
    if (res.has_failure()) {
        vlog(
          _log.info,
          "Failed to observe replicated command in log, reason: {}",
          res.error().message());
        co_return res.as_failure();
    }
    co_return res.value();
}

ss::future<log_eviction_stm::offset_result> log_eviction_stm::replicate_command(
  model::record_batch batch,
  ss::lowres_clock::time_point deadline,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    auto opts = raft::replicate_options(raft::consistency_level::quorum_ack);
    opts.set_force_flush();
    auto fut = _raft->replicate(
      _raft->term(),
      model::make_memory_record_batch_reader(std::move(batch)),
      opts);

    /// Execute the replicate command bound by timeout and cancellable via
    /// abort_source mechanism
    result<raft::replicate_result> result{{}};
    try {
        if (as) {
            result = co_await ssx::with_timeout_abortable(
              std::move(fut), deadline, *as);
        } else {
            result = co_await ss::with_timeout(deadline, std::move(fut));
        }
    } catch (const ss::timed_out_error&) {
        result = errc::timeout;
    } catch (...) {
        vlog(
          _log.warn,
          "Replicating prefix_truncate failed with exception: {}",
          std::current_exception());
        result = errc::replication_error;
    }

    if (!result) {
        vlog(
          _log.warn,
          "Failed to replicate prefix_truncate command, reason: {}",
          result.error());
        co_return result.error();
    }

    /// The following will return when the command replicated above has been
    /// processed by the apply() method. This effectively bumps the start offset
    /// to the requested value and since apply is deterministic this is
    /// guaranteed to occur. No guarantees of data removal / availability can be
    /// made at or after this point, since that occurs in a background fiber.
    auto applied = co_await wait_no_throw(
      result.value().last_offset, deadline, as);
    if (!applied) {
        if (as && as->get().abort_requested()) {
            co_return errc::shutting_down;
        }
        co_return errc::timeout;
    }
    co_return result.value().last_offset;
}

ss::future<> log_eviction_stm::do_apply(const model::record_batch& batch) {
    if (likely(
          batch.header().type != model::record_batch_type::prefix_truncate)) {
        co_return;
    }
    /// The work done within apply() must be deterministic that way the start
    /// offset will always be the same value across all replicas
    ///
    /// Here all apply() does is move forward the in memory start offset, a
    /// background fiber is responsible for evicting as much as possible
    /// record_batches of type ::prefix_truncate are always of size 1
    const auto batch_type = serde::from_iobuf<uint8_t>(
      batch.copy_records().begin()->release_key());
    if (batch_type != prefix_truncate_key) {
        vlog(
          _log.error,
          "Unknown prefix_truncate batch type at offset {}: {}",
          batch.header().base_offset(),
          batch_type);
        co_return;
    }
    const auto record = serde::from_iobuf<prefix_truncate_record>(
      batch.copy_records().begin()->release_value());
    _cached_kafka_start_offset_override = record.kafka_start_offset;
    if (record.rp_start_offset == model::offset{}) {
        // This may happen if the requested offset was not in the local log at
        // time of replicating. We still need to have replicated it though so
        // other STMs can honor it (e.g. archival).
        vlog(
          _log.info,
          "Replicated prefix_truncate batch with no local redpanda "
          "offset. Requested start Kafka offset {}",
          record.kafka_start_offset);
        co_return;
    }
    auto truncate_offset = record.rp_start_offset - model::offset(1);
    if (truncate_offset > _delete_records_eviction_offset) {
        vlog(
          _log.info,
          "Applying prefix truncate batch with truncate offset: {} "
          "last_applied: {}",
          truncate_offset,
          last_applied_offset());

        /// Set the delete records offset.
        _delete_records_eviction_offset = truncate_offset;
        _has_pending_truncation.signal();
    }
}

ss::future<> log_eviction_stm::apply_raft_snapshot(const iobuf&) {
    auto last_snapshot_index = model::prev_offset(_raft->start_offset());
    _delete_records_eviction_offset = model::offset{};
    _storage_eviction_offset = last_snapshot_index;
    set_next(model::next_offset(last_snapshot_index));
    vlog(
      _log.info,
      "Handled log eviction new effective start offset: {}",
      effective_start_offset());
    co_return;
}

ss::future<> log_eviction_stm::apply_local_snapshot(
  raft::stm_snapshot_header header, iobuf&& data) {
    auto snapshot = serde::from_iobuf<snapshot_data>(std::move(data));
    vlog(
      _log.info, "Applying snapshot {} at offset: {}", snapshot, header.offset);

    _delete_records_eviction_offset = snapshot.effective_start_offset;
    return ss::now();
}

ss::future<raft::stm_snapshot>
log_eviction_stm::take_local_snapshot(ssx::semaphore_units apply_units) {
    vlog(_log.trace, "Taking snapshot at offset: {}", last_applied_offset());
    iobuf snap_data = serde::to_iobuf(
      snapshot_data{.effective_start_offset = _delete_records_eviction_offset});
    apply_units.return_all();
    co_return raft::stm_snapshot::create(
      0, last_applied_offset(), std::move(snap_data));
}

ss::future<> log_eviction_stm::ensure_local_snapshot_exists(model::offset) {
    /// This class drives eviction, therefore it cannot wait until its own
    /// snapshot exists until writing a snapshot
    return ss::now();
}

log_eviction_stm_factory::log_eviction_stm_factory(storage::kvstore& kvstore)
  : _kvstore(kvstore) {}

bool log_eviction_stm_factory::is_applicable_for(
  const storage::ntp_config& cfg) const {
    return !storage::deletion_exempt(cfg.ntp());
}

void log_eviction_stm_factory::create(
  raft::state_machine_manager_builder& builder, raft::consensus* raft) {
    auto stm = builder.create_stm<log_eviction_stm>(raft, clusterlog, _kvstore);
    raft->log()->stm_manager()->add_stm(stm);
}

} // namespace cluster
