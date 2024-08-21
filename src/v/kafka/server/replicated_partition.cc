/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "kafka/server/replicated_partition.h"

#include "cloud_storage/types.h"
#include "cluster/partition.h"
#include "cluster/rm_stm.h"
#include "kafka/protocol/errors.h"
#include "kafka/server/errors.h"
#include "kafka/server/logger.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "raft/consensus_utils.h"
#include "raft/errc.h"
#include "storage/log_reader.h"
#include "storage/types.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

#include <optional>

namespace kafka {
replicated_partition::replicated_partition(
  ss::lw_shared_ptr<cluster::partition> p) noexcept
  : _partition(p)
  , _translator(_partition->get_offset_translator_state()) {
    vassert(
      _translator, "ntp {}: offset translator must be initialized", p->ntp());
}

const model::ntp& replicated_partition::ntp() const {
    return _partition->ntp();
}

ss::future<result<model::offset, error_code>>
replicated_partition::sync_effective_start(
  model::timeout_clock::duration timeout) {
    return _partition->sync_kafka_start_offset_override(timeout).then(
      [this](auto synced_start_offset_override)
        -> result<model::offset, error_code> {
          if (synced_start_offset_override.has_failure()) {
              auto err = synced_start_offset_override.error();
              auto error_code = error_code::unknown_server_error;
              if (err.category() == cluster::error_category()) {
                  switch (cluster::errc(err.value())) {
                      /**
                       * In the case of timeout and shutting down errors return
                       * not_leader_for_partition error to force clients retry.
                       */
                  case cluster::errc::shutting_down:
                  case cluster::errc::not_leader:
                  case cluster::errc::timeout:
                      error_code = error_code::not_leader_for_partition;
                      break;
                  default:
                      error_code = error_code::unknown_server_error;
                  }
              }
              return error_code;
          }
          return kafka_start_offset_with_override(
            synced_start_offset_override.value());
      });
}

model::offset replicated_partition::start_offset() const {
    const auto start_offset_override
      = _partition->kafka_start_offset_override();
    if (!start_offset_override.has_value()) {
        return partition_kafka_start_offset();
    }
    return kafka_start_offset_with_override(start_offset_override.value());
}

model::offset replicated_partition::high_watermark() const {
    if (_partition->is_read_replica_mode_enabled()) {
        if (_partition->cloud_data_available()) {
            return _partition->next_cloud_offset();
        } else {
            return model::offset(0);
        }
    }
    return _translator->from_log_offset(_partition->high_watermark());
}
/**
 * According to Kafka protocol semantics a log_end_offset is an offset that
 * is assigned to the next record produced to a log
 */
model::offset replicated_partition::log_end_offset() const {
    if (_partition->is_read_replica_mode_enabled()) {
        if (_partition->cloud_data_available()) {
            return model::next_offset(_partition->next_cloud_offset());
        } else {
            return model::offset(0);
        }
    }
    /**
     * If a local log is empty we return start offset as this is the offset
     * assigned to the next batch produced to the log.
     */
    if (_partition->dirty_offset() < _partition->raft_start_offset()) {
        return _translator->from_log_offset(_partition->raft_start_offset());
    }
    /**
     * By default we return a dirty_offset + 1
     */
    return _translator->from_log_offset(
      model::next_offset(_partition->dirty_offset()));
}

model::offset replicated_partition::leader_high_watermark() const {
    if (_partition->is_read_replica_mode_enabled()) {
        return high_watermark();
    }
    return _translator->from_log_offset(_partition->leader_high_watermark());
}

checked<model::offset, error_code>
replicated_partition::last_stable_offset() const {
    if (_partition->is_read_replica_mode_enabled()) {
        if (_partition->cloud_data_available()) {
            // There is no difference between HWM and LO in this mode
            return _partition->next_cloud_offset();
        } else {
            return model::offset(0);
        }
    }
    auto maybe_lso = _partition->last_stable_offset();
    if (maybe_lso == model::invalid_lso) {
        return error_code::offset_not_available;
    }
    return _translator->from_log_offset(maybe_lso);
}

bool replicated_partition::is_leader() const { return _partition->is_leader(); }

ss::future<std::error_code> replicated_partition::linearizable_barrier() {
    auto r = co_await _partition->linearizable_barrier();
    if (r) {
        co_return raft::errc::success;
    }
    co_return r.error();
}

cluster::partition_probe& replicated_partition::probe() {
    return _partition->probe();
}

kafka::leader_epoch replicated_partition::leader_epoch() const {
    return leader_epoch_from_term(_partition->raft()->confirmed_term());
}

// TODO: use previous translation speed up lookup
ss::future<storage::translating_reader> replicated_partition::make_reader(
  storage::log_reader_config cfg,
  std::optional<model::timeout_clock::time_point> debounce_deadline) {
    if (
      _partition->is_read_replica_mode_enabled()
      && _partition->cloud_data_available()) {
        // No need to translate the offsets in this case since all fetch
        // requestS in read replica are served via remote_partition which
        // does its own translation.
        co_return co_await _partition->make_cloud_reader(cfg);
    }

    if (
      may_read_from_cloud(model::offset_cast(cfg.start_offset))
      && cfg.start_offset >= _partition->start_cloud_offset()) {
        cfg.type_filter = {model::record_batch_type::raft_data};
        co_return co_await _partition->make_cloud_reader(
          cfg, debounce_deadline);
    }

    cfg.start_offset = _translator->to_log_offset(cfg.start_offset);
    cfg.max_offset = _translator->to_log_offset(cfg.max_offset);
    cfg.type_filter = {model::record_batch_type::raft_data};

    cfg.translate_offsets = storage::translate_offsets::yes;
    auto rdr = co_await _partition->make_reader(cfg, debounce_deadline);
    co_return storage::translating_reader(std::move(rdr), _translator);
}

ss::future<std::vector<cluster::tx::tx_range>>
replicated_partition::aborted_transactions_local(
  cloud_storage::offset_range offsets,
  ss::lw_shared_ptr<const storage::offset_translator_state> ot_state) {
    // Note: here we expect that local _partition contains aborted transaction
    // ids for both local and remote offset ranges. This is true as long as
    // rm_stm state has not been reset (for example when there is a partition
    // transfer or when a stale replica recovers its log from beyond the log
    // eviction point). See
    // https://github.com/redpanda-data/redpanda/issues/3001

    auto source = co_await _partition->aborted_transactions(
      offsets.begin_rp, offsets.end_rp);

    // We trim beginning of aborted ranges to `trim_at` because we don't have
    // offset translation info for earlier offsets.
    model::offset trim_at;
    if (offsets.begin_rp >= _partition->raft_start_offset()) {
        // Local fetch. Trim to start of the log - it is safe because clients
        // can't read earlier offsets.
        trim_at = _partition->raft_start_offset();
    } else {
        // Fetch from cloud data. Trim to start of the read range - this is
        // incorrect because clients can still see earlier offsets but will work
        // if they won't use aborted ranges from this request to filter batches
        // belonging to earlier offsets.
        trim_at = offsets.begin_rp;
    }

    std::vector<cluster::tx::tx_range> target;
    target.reserve(source.size());
    for (const auto& range : source) {
        target.emplace_back(
          range.pid,
          ot_state->from_log_offset(std::max(trim_at, range.first)),
          ot_state->from_log_offset(range.last));
    }

    co_return target;
}

ss::future<std::vector<cluster::tx::tx_range>>
replicated_partition::aborted_transactions_remote(
  cloud_storage::offset_range offsets,
  ss::lw_shared_ptr<const storage::offset_translator_state> ot_state) {
    auto source = co_await _partition->aborted_transactions_cloud(offsets);
    std::vector<cluster::tx::tx_range> target;
    target.reserve(source.size());
    for (const auto& range : source) {
        target.emplace_back(
          range.pid,
          ot_state->from_log_offset(std::max(offsets.begin_rp, range.first)),
          ot_state->from_log_offset(range.last));
    }
    co_return target;
}

/**
 * Based on the lower offset of an incoming request, decide whether it should
 * be sent to cloud storage (return true), or local raft storage (return false)
 */
bool replicated_partition::may_read_from_cloud(kafka::offset start_offset) {
    return _partition->is_remote_fetch_enabled()
           && _partition->cloud_data_available()
           && (start_offset < model::offset_cast(_translator->from_log_offset(_partition->raft_start_offset())));
}

ss::future<std::vector<cluster::tx::tx_range>>
replicated_partition::aborted_transactions(
  model::offset base,
  model::offset last,
  ss::lw_shared_ptr<const storage::offset_translator_state> ot_state) {
    // We can extract information about aborted transactions from local raft log
    // or from the S3 bucket. The decision is made using the following logic:
    // - if the record batches were produced by shadow indexing (downloaded from
    // S3)
    //   then we should use the same source for transactions metadata. It's
    //   guaranteed that in this case we will find the corresponding manifest
    //   (it's downloaded alongside the segment to SI cache). This also means
    //   that we will have the manifests hydrated on disk (since we just
    //   downloaded corresponding segments from S3 to produce batches).
    // - if the source of data is local raft log then we should use abroted
    // transactions
    //   snapshot.
    //
    // Sometimes the snapshot will have data for the offset range even if the
    // source is S3 bucket. In this case we won't be using this data because
    // it's not guaranteed that it has the data for the entire offset range and
    // we won't be able to tell the difference by looking at the results (for
    // instance, the offset range is 0-100, but the snapshot has data starting
    // from offset 50, it will return data for range 50-100 and we won't be able
    // to tell if it didn't have data for 0-50 or there wasn't any transactions
    // in that range).
    vassert(ot_state, "ntp {}: offset translator state must be present", ntp());
    auto base_rp = ot_state->to_log_offset(base);
    auto last_rp = ot_state->to_log_offset(last);
    cloud_storage::offset_range offsets = {
      .begin = model::offset_cast(base),
      .end = model::offset_cast(last),
      .begin_rp = base_rp,
      .end_rp = last_rp,
    };
    if (_partition->is_read_replica_mode_enabled()) {
        // Always use SI for read replicas
        co_return co_await aborted_transactions_remote(offsets, ot_state);
    }
    if (may_read_from_cloud(model::offset_cast(base))) {
        // The fetch request was satisfied using shadow indexing.
        auto tx_remote = co_await aborted_transactions_remote(
          offsets, ot_state);
        if (!tx_remote.empty()) {
            // NOTE: we don't have a way to upload tx-manifests to the cloud
            // for segments which was uploaded by old redpanda version because
            // we can't guarantee that the local snapshot still has the data.
            // This means that 'aborted_transaction_remote' might return empty
            // result in case if the segment was uploaded by previous version of
            // redpanda. In this case we will try to fetch the aborted
            // transactions metadata from local snapshot. This approach provide
            // the same guarantees that we have in v22.1 for data produced by
            // v22.1 and earlier. But for new data we will guarantee that the
            // metadata is always available in S3.
            co_return tx_remote;
        }
    }
    co_return co_await aborted_transactions_local(offsets, ot_state);
}

ss::future<std::optional<storage::timequery_result>>
replicated_partition::timequery(storage::timequery_config cfg) {
    // cluster::partition::timequery returns a result in Kafka offsets,
    // no further offset translation is required here.
    return _partition->timequery(cfg);
}

ss::future<result<model::offset>> replicated_partition::replicate(
  model::record_batch_reader rdr, raft::replicate_options opts) {
    using ret_t = result<model::offset>;
    if (_partition->is_read_replica_mode_enabled()) {
        return ss::make_ready_future<ret_t>(
          kafka::error_code::invalid_topic_exception);
    }
    return _partition->replicate(std::move(rdr), opts)
      .then([](result<cluster::kafka_result> r) {
          if (!r) {
              return ret_t(r.error());
          }
          return ret_t(model::offset(r.value().last_offset()));
      });
}

raft::replicate_stages replicated_partition::replicate(
  model::batch_identity batch_id,
  model::record_batch_reader&& rdr,
  raft::replicate_options opts) {
    using ret_t = result<raft::replicate_result>;
    if (_partition->is_read_replica_mode_enabled()) {
        return {
          ss::now(),
          ss::make_ready_future<result<raft::replicate_result>>(
            make_error_code(kafka::error_code::invalid_topic_exception))};
    }

    auto res = _partition->replicate_in_stages(batch_id, std::move(rdr), opts);

    raft::replicate_stages out(raft::errc::success);
    out.request_enqueued = std::move(res.request_enqueued);
    out.replicate_finished = res.replicate_finished.then(
      [](result<cluster::kafka_result> r) {
          if (!r) {
              return ret_t(r.error());
          }
          return ret_t(
            raft::replicate_result{model::offset(r.value().last_offset())});
      });
    return out;
}

model::offset replicated_partition::partition_kafka_start_offset() const {
    if (
      _partition->is_read_replica_mode_enabled()
      && _partition->cloud_data_available()) {
        // Always assume remote read in this case.
        return _partition->start_cloud_offset();
    }

    auto local_kafka_start_offset = _translator->from_log_offset(
      _partition->raft_start_offset());
    if (
      _partition->is_remote_fetch_enabled()
      && _partition->cloud_data_available()
      && (_partition->start_cloud_offset() < local_kafka_start_offset)) {
        return _partition->start_cloud_offset();
    }
    return local_kafka_start_offset;
}

model::offset replicated_partition::kafka_start_offset_with_override(
  model::offset start_kafka_offset_override) const {
    if (start_kafka_offset_override == model::offset{}) {
        return partition_kafka_start_offset();
    }
    if (_partition->is_read_replica_mode_enabled()) {
        // The start override may fall ahead of the HWM since read replicas
        // compute HWM based on uploaded segments, and the override may
        // appear in the manifest before uploading corresponding segments.
        // Clamp down to the HWM.
        const auto hwm = high_watermark();
        if (hwm <= start_kafka_offset_override) {
            return hwm;
        }
    }
    return std::max(
      partition_kafka_start_offset(), start_kafka_offset_override);
}

ss::future<std::optional<model::offset>>
replicated_partition::get_leader_epoch_last_offset(
  kafka::leader_epoch epoch) const {
    auto offset_unbounded = co_await get_leader_epoch_last_offset_unbounded(
      epoch);
    if (!offset_unbounded.has_value()) {
        co_return std::nullopt;
    }
    if (!_partition->kafka_start_offset_override().has_value()) {
        co_return offset_unbounded;
    }
    // If the requested term falls below our earliest consumable segment as
    // bounded by a start override, return the offset of the next-highest term
    // (the new start offset).
    co_return std::max(offset_unbounded.value(), start_offset());
}

ss::future<std::optional<model::offset>>
replicated_partition::get_leader_epoch_last_offset_unbounded(
  kafka::leader_epoch epoch) const {
    const model::term_id term(epoch);
    const auto first_local_offset = _partition->raft_start_offset();
    const auto first_local_term = _partition->get_term(first_local_offset);
    const auto last_local_term = _partition->term();
    const auto is_read_replica = _partition->is_read_replica_mode_enabled();

    vlog(
      klog.debug,
      "{} get_leader_epoch_last_offset_unbounded, term {}, first local offset "
      "{}, "
      "first local term {}, last local term {}, is read replica {}",
      _partition->get_ntp_config().ntp(),
      term,
      first_local_offset,
      first_local_term,
      last_local_term,
      is_read_replica);

    if (!is_read_replica && term > last_local_term) {
        // Request for term that is in the future
        co_return std::nullopt;
    }
    // Look for the highest offset in the requested term, or the first offset
    // in the next term. This mirrors behavior in Kafka, see
    // https://github.com/apache/kafka/blob/97105a8e5812135515f5a0fa4d5ff554d80df2fe/storage/src/main/java/org/apache/kafka/storage/internals/epoch/LeaderEpochFileCache.java#L255-L281
    if (!is_read_replica && term >= first_local_term) {
        auto last_offset = _partition->get_term_last_offset(term);
        if (last_offset) {
            co_return _translator->from_log_offset(*last_offset);
        }
    }
    // The requested term falls below our earliest local segment.

    // Check cloud storage for a viable offset.
    if (
      is_read_replica
      || (_partition->is_remote_fetch_enabled() && _partition->cloud_data_available())) {
        if (is_read_replica && !_partition->cloud_data_available()) {
            // If we didn't sync the manifest yet the cloud_data_available will
            // return false. We can't call `get_cloud_term_last_offset` in this
            // case but we also can't use `first_local_offset` for read replica.
            co_return std::nullopt;
        }
        auto last_offset = co_await _partition->get_cloud_term_last_offset(
          term);
        if (last_offset) {
            co_return last_offset;
        } else {
            // Return the offset of this next-highest term, but from the
            // cloud
            co_return _partition->start_cloud_offset();
        }
    }

    // Return the offset of this next-highest term.
    co_return _translator->from_log_offset(first_local_offset);
}

ss::future<error_code> replicated_partition::prefix_truncate(
  model::offset kafka_truncation_offset,
  ss::lowres_clock::time_point deadline) {
    // truncation_offset < 0 cases have already been checked in
    // `kafka::prefix_truncate()` handler.
    if (kafka_truncation_offset <= start_offset()) {
        // No-op, return early.
        co_return kafka::error_code::none;
    }
    if (kafka_truncation_offset > high_watermark()) {
        co_return error_code::offset_out_of_range;
    }
    model::offset rp_truncate_offset{};
    auto local_kafka_start_offset = _translator->from_log_offset(
      _partition->raft_start_offset());
    if (kafka_truncation_offset > local_kafka_start_offset) {
        rp_truncate_offset = _translator->to_log_offset(
          kafka_truncation_offset);
    }
    auto errc = co_await _partition->prefix_truncate(
      rp_truncate_offset,
      model::offset_cast(kafka_truncation_offset),
      deadline);

    if (errc.category() == raft::error_category()) {
        switch (raft::errc(errc.value())) {
        case raft::errc::success:
            co_return kafka::error_code::none;
        case raft::errc::not_leader:
            co_return kafka::error_code::not_leader_for_partition;
        case raft::errc::shutting_down:
            // it's not clear if the request succeeded or not before the
            // shutdown so mark this request as timed out. This is similar to
            // what the produce handler does.
            co_return kafka::error_code::request_timed_out;
        default:
            vlog(
              klog.error, "Unhandled raft error encountered: {}", errc.value());
            co_return error_code::unknown_server_error;
        }
    } else if (errc.category() != cluster::error_category()) {
        vlog(
          klog.error,
          "Unhandled error_category encountered: {}",
          errc.category().name());
        co_return error_code::unknown_server_error;
    }
    co_return map_topic_error_code(cluster::errc(errc.value()));
}

ss::future<error_code> replicated_partition::validate_fetch_offset(
  model::offset fetch_offset,
  bool reading_from_follower,
  model::timeout_clock::time_point deadline) {
    /**
     * Make sure that we will update high watermark offset if it isn't yet
     * initialized.
     *
     * We use simple heuristic here to determine if high watermark update is
     * required.
     *
     * We only request update if fetch_offset is in range beteween current high
     * water mark and log end i.e. if high watermark is updated consumer will
     * receive data.
     */

    // offset validation logic on follower
    if (reading_from_follower && !_partition->is_leader()) {
        auto ec = error_code::none;

        const auto available_to_read = std::min(
          leader_high_watermark(), log_end_offset());

        if (fetch_offset < start_offset()) {
            ec = error_code::offset_out_of_range;
        } else if (fetch_offset > available_to_read) {
            /**
             * Offset know to be committed but not yet available on the
             * follower.
             */
            ec = error_code::offset_not_available;
        }

        if (ec != error_code::none) {
            vlog(
              klog.warn,
              "ntp {}: fetch offset out of range on follower, requested: {}, "
              "partition start offset: {}, high watermark: {}, leader high "
              "watermark: {}, log end offset: {}, ec: {}",
              ntp(),
              fetch_offset,
              start_offset(),
              high_watermark(),
              leader_high_watermark(),
              log_end_offset(),
              ec);
        }

        return ss::make_ready_future<error_code>(ec);
    }

    // Grab the up to date start offset
    auto timeout = deadline - model::timeout_clock::now();
    return sync_effective_start(timeout).then(
      [this, fetch_offset](auto start_offset) {
          if (!start_offset) {
              vlog(
                klog.warn,
                "ntp {}: error obtaining latest start offset - {}",
                ntp(),
                start_offset.error());
              return start_offset.error();
          }

          if (
            fetch_offset < start_offset.value()
            || fetch_offset > log_end_offset()) {
              vlog(
                klog.warn,
                "ntp {}: fetch offset_out_of_range on leader, requested: {}, "
                "partition start offset: {}, high watermark: {}, log end "
                "offset: {}",
                ntp(),
                fetch_offset,
                start_offset.value(),
                high_watermark(),
                log_end_offset());
              return error_code::offset_out_of_range;
          }

          return error_code::none;
      });
}

result<partition_info> replicated_partition::get_partition_info() const {
    partition_info ret;
    ret.leader = _partition->get_leader_id();
    ret.replicas.reserve(_partition->raft()->get_follower_count() + 1);
    auto followers = _partition->get_follower_metrics();

    if (followers.has_error()) {
        return followers.error();
    }
    auto start_offset = _partition->raft_start_offset();

    auto clamped_translate = [this, start_offset](model::offset to_translate) {
        return to_translate >= start_offset
                 ? _translator->from_log_offset(to_translate)
                 : _translator->from_log_offset(start_offset);
    };

    for (const auto& follower_metric : followers.value()) {
        ret.replicas.push_back(replica_info{
          .id = follower_metric.id,
          .high_watermark = model::next_offset(
            clamped_translate(follower_metric.match_index)),
          .log_end_offset = model::next_offset(
            clamped_translate(follower_metric.dirty_log_index)),
          .is_alive = follower_metric.is_live,
        });
    }

    ret.replicas.push_back(replica_info{
      .id = _partition->raft()->self().id(),
      .high_watermark = high_watermark(),
      .log_end_offset = log_end_offset(),
      .is_alive = true,
    });

    return {std::move(ret)};
}

} // namespace kafka
