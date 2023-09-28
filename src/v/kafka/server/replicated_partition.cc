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
#include "cluster/errc.h"
#include "kafka/protocol/errors.h"
#include "kafka/server/errors.h"
#include "kafka/server/logger.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "raft/consensus_utils.h"
#include "raft/errc.h"
#include "raft/types.h"
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

// TODO: use previous translation speed up lookup
ss::future<storage::translating_reader> replicated_partition::make_reader(
  storage::log_reader_config cfg,
  std::optional<model::timeout_clock::time_point> deadline) {
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
        co_return co_await _partition->make_cloud_reader(cfg, deadline);
    }

    cfg.start_offset = _translator->to_log_offset(cfg.start_offset);
    cfg.max_offset = _translator->to_log_offset(cfg.max_offset);
    cfg.type_filter = {model::record_batch_type::raft_data};

    class reader : public model::record_batch_reader::impl {
    public:
        reader(
          std::unique_ptr<model::record_batch_reader::impl> underlying,
          ss::lw_shared_ptr<const storage::offset_translator_state> tr)
          : _underlying(std::move(underlying))
          , _translator(std::move(tr)) {}

        bool is_end_of_stream() const final {
            return _underlying->is_end_of_stream();
        }

        void print(std::ostream& os) final {
            fmt::print(os, "kafka::partition reader for ");
            _underlying->print(os);
        }
        using storage_t = model::record_batch_reader::storage_t;
        using data_t = model::record_batch_reader::data_t;
        using foreign_data_t = model::record_batch_reader::foreign_data_t;

        model::record_batch_reader::data_t& get_batches(storage_t& st) {
            if (std::holds_alternative<data_t>(st)) {
                return std::get<data_t>(st);
            } else {
                return *std::get<foreign_data_t>(st).buffer;
            }
        }

        ss::future<storage_t>
        do_load_slice(model::timeout_clock::time_point t) final {
            return _underlying->do_load_slice(t).then([this](storage_t recs) {
                for (auto& batch : get_batches(recs)) {
                    batch.header().base_offset = _translator->from_log_offset(
                      batch.base_offset());
                }
                return recs;
            });
        }

        ss::future<> finally() noexcept final { return _underlying->finally(); }

    private:
        std::unique_ptr<model::record_batch_reader::impl> _underlying;
        ss::lw_shared_ptr<const storage::offset_translator_state> _translator;
    };
    auto rdr = co_await _partition->make_reader(cfg, deadline);
    co_return storage::translating_reader(
      model::make_record_batch_reader<reader>(
        std::move(rdr).release(), _translator),
      _translator);
}

ss::future<std::vector<cluster::rm_stm::tx_range>>
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

    std::vector<cluster::rm_stm::tx_range> target;
    target.reserve(source.size());
    for (const auto& range : source) {
        target.push_back(cluster::rm_stm::tx_range{
          .pid = range.pid,
          .first = ot_state->from_log_offset(std::max(trim_at, range.first)),
          .last = ot_state->from_log_offset(range.last)});
    }

    co_return target;
}

ss::future<std::vector<cluster::rm_stm::tx_range>>
replicated_partition::aborted_transactions_remote(
  cloud_storage::offset_range offsets,
  ss::lw_shared_ptr<const storage::offset_translator_state> ot_state) {
    auto source = co_await _partition->aborted_transactions_cloud(offsets);
    std::vector<cluster::rm_stm::tx_range> target;
    target.reserve(source.size());
    for (const auto& range : source) {
        target.push_back(cluster::rm_stm::tx_range{
          .pid = range.pid,
          .first = ot_state->from_log_offset(
            std::max(offsets.begin_rp, range.first)),
          .last = ot_state->from_log_offset(range.last)});
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
           && (start_offset < _translator->from_log_offset(_partition->raft_start_offset()));
}

ss::future<std::vector<cluster::rm_stm::tx_range>>
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
    auto res = co_await _partition->timequery(cfg);
    if (!res.has_value()) {
        co_return std::nullopt;
    }
    const auto kafka_start_override = _partition->kafka_start_offset_override();
    if (
      !kafka_start_override.has_value()
      || kafka_start_override.value() <= res.value().offset) {
        // The start override doesn't affect the result of the timequery.
        co_return res;
    }
    vlog(
      klog.debug,
      "{} timequery result {} clamped by start override, fetching result at "
      "start {}",
      ntp(),
      res->offset,
      kafka_start_override.value());
    storage::log_reader_config config(
      kafka_start_override.value(),
      cfg.max_offset,
      0,
      2048, // We just need one record batch
      cfg.prio,
      cfg.type_filter,
      std::nullopt, // No timestamp, just use the offset
      cfg.abort_source);
    auto translating_reader = co_await make_reader(config, std::nullopt);
    auto ot_state = std::move(translating_reader.ot_state);
    model::record_batch_reader::storage_t data
      = co_await model::consume_reader_to_memory(
        std::move(translating_reader.reader), model::no_timeout);
    auto& batches = std::get<model::record_batch_reader::data_t>(data);
    if (batches.empty()) {
        co_return std::nullopt;
    }
    co_return storage::batch_timequery(*(batches.begin()), cfg.time);
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
    if (term > last_local_term) {
        // Request for term that is in the future
        co_return std::nullopt;
    }
    // Look for the highest offset in the requested term, or the first offset
    // in the next term. This mirrors behavior in Kafka, see
    // https://github.com/apache/kafka/blob/97105a8e5812135515f5a0fa4d5ff554d80df2fe/storage/src/main/java/org/apache/kafka/storage/internals/epoch/LeaderEpochFileCache.java#L255-L281
    if (term >= first_local_term) {
        auto last_offset = _partition->get_term_last_offset(term);
        if (last_offset) {
            co_return _translator->from_log_offset(*last_offset);
        }
    }
    // The requested term falls below our earliest local segment.

    // Check cloud storage for a viable offset.
    if (
      _partition->is_remote_fetch_enabled()
      && _partition->cloud_data_available()) {
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
    if (
      kafka_truncation_offset <= start_offset()
      || kafka_truncation_offset > high_watermark()) {
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
        if (fetch_offset < start_offset()) {
            co_return error_code::offset_out_of_range;
        }
        if (
          fetch_offset > high_watermark()
          && fetch_offset <= leader_high_watermark()) {
            co_return error_code::offset_not_available;
        }
        if (fetch_offset > leader_high_watermark()) {
            co_return error_code::offset_out_of_range;
        }
        co_return error_code::none;
    }

    // Grab the up to date start offset
    auto timeout = deadline - model::timeout_clock::now();
    auto start_offset = co_await sync_effective_start(timeout);
    if (!start_offset) {
        vlog(
          klog.warn,
          "error obtaining latest start offset - {}",
          start_offset.error());
        co_return start_offset.error();
    }

    co_return fetch_offset >= start_offset.value()
        && fetch_offset <= log_end_offset()
      ? error_code::none
      : error_code::offset_out_of_range;
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
