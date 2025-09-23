/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/frontend/frontend.h"

#include "cloud_storage/types.h"
#include "cloud_topics/data_plane_api.h"
#include "cloud_topics/frontend/errc.h"
#include "cloud_topics/level_zero/common/extent_meta.h"
#include "cloud_topics/level_zero/frontend_reader/reader.h"
#include "cloud_topics/level_zero/stm/ctp_stm.h"
#include "cloud_topics/level_zero/stm/placeholder.h"
#include "cloud_topics/logger.h"
#include "cluster/partition.h"
#include "cluster/rm_stm_types.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/record_batch_types.h"
#include "model/timeout_clock.h"
#include "raft/errc.h"
#include "raft/replicate.h"
#include "storage/record_batch_builder.h"
#include "storage/types.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/defer.hh>

#include <chrono>
#include <iterator>
#include <optional>
#include <stdexcept>

namespace cloud_topics {

namespace {

struct placeholder_batches_with_size {
    ss::circular_buffer<model::record_batch> batches;
    // Total size of all referenced data
    size_t extent_size{0};
};

static constexpr auto L0_upload_default_timeout = 1s;
static constexpr auto L0_replicate_default_timeout = 1s;

// Create a placeholder batch using the original header and the extent.
// The caller is supposed to use the correct batch header
// that matches the extent.
static model::record_batch make_placeholder_batch(
  const model::record_batch_header& hdr,
  const cloud_topics::extent_meta& extent) {
    vassert(hdr.record_count > 0, "Empty record batch not allowed {}", hdr);

    cloud_topics::dl_placeholder placeholder{
      .id = extent.id,
      .offset = extent.first_byte_offset,
      .size_bytes = extent.byte_range_size,
    };

    storage::record_batch_builder builder(
      model::record_batch_type::dl_placeholder, hdr.base_offset);

    builder.set_producer_identity(hdr.producer_id, hdr.producer_epoch);
    if (hdr.attrs.is_control()) {
        builder.set_control_type();
    }
    if (hdr.attrs.is_transactional()) {
        builder.set_transactional_type();
    }

    auto first_key = serde::to_iobuf(
      cloud_topics::dl_placeholder_record_key::payload);

    auto first_value = serde::to_iobuf(placeholder);

    // In case of a placeholder batch the first record contains the
    // actual placeholder and the remaining records are empty. The remaining
    // records are added to avoid confusing any other code that may expect
    // that the number of records in the batch is equal to the number of
    // offsets in the header.
    builder.add_raw_kv(std::move(first_key), std::move(first_value));

    for (int i = 1; i < hdr.record_count; ++i) {
        builder.add_raw_kv(std::nullopt, std::nullopt);
    }

    auto ph = std::move(builder).build();
    ph.header().first_timestamp = hdr.first_timestamp;
    ph.header().max_timestamp = hdr.max_timestamp;
    ph.header().base_sequence = hdr.base_sequence;
    ph.header().reset_size_checksum_metadata(ph.data());
    return ph;
}

// Utility function to convert array of extent_meta structs to
// array of placeholder batches.
static placeholder_batches_with_size convert_to_placeholders(
  const chunked_vector<cloud_topics::extent_meta>& extents,
  const chunked_vector<model::record_batch_header>& original_headers) {
    // TODO: avoid copying this buffer
    ss::circular_buffer<model::record_batch_header> headers;
    std::copy(
      original_headers.begin(),
      original_headers.end(),
      std::back_inserter(headers));
    placeholder_batches_with_size result;
    result.batches.reserve(extents.size());
    for (const auto& extent : extents) {
        auto header = headers.front();
        headers.pop_front();
        vassert(
          extent.base_offset() <= extent.last_offset(),
          "Extent base offset {} is greater than committed offset {}",
          extent.base_offset(),
          extent.last_offset());

        // Every extent maps to a single batch produced by the client
        // and therefore we need to create a placeholder batch for it.
        auto batch = make_placeholder_batch(header, extent);

        result.batches.push_back(std::move(batch));
        result.extent_size += extent.byte_range_size;
    }
    return result;
}

/// Get original record batch and prepare it for the record batch cache
static void update_batch_base_offset(
  model::record_batch& src, model::offset offset, model::term_id term) {
    src.set_term(term);
    src.header().base_offset = offset;
    src.header().reset_size_checksum_metadata(src.data());
}

static chunked_vector<model::record_batch>
clone_batches(const chunked_vector<model::record_batch>& src) {
    chunked_vector<model::record_batch> res;
    for (auto& s : src) {
        res.push_back(s.copy());
    }
    return res;
}

/// Write proper offsets into the record batches
static void update_batches(
  chunked_vector<model::record_batch>& src,
  model::offset last_offset,
  model::term_id term) {
    chunked_vector<model::record_batch> ret;

    int64_t num_records = 0;
    for (const auto& s : src) {
        num_records += s.record_count();
    }

    auto o = model::offset(last_offset() - (num_records - 1));
    for (auto& s : src) {
        update_batch_base_offset(s, o, term);
        o = model::next_offset(s.last_offset());
    }
}

static ss::lw_shared_ptr<cloud_topics::ctp_stm_api> make_ctp_stm_api(
  retry_chain_node& rtc, ss::lw_shared_ptr<cluster::partition> p) {
    auto stm = p->raft()->stm_manager()->get<cloud_topics::ctp_stm>();
    if (!stm) {
        throw std::runtime_error(
          fmt::format("ctp_stm not found for partition {}", p->ntp()));
    }
    return ss::make_lw_shared<cloud_topics::ctp_stm_api>(rtc, stm);
}

} // namespace

frontend::frontend(
  ss::lw_shared_ptr<cluster::partition> p, data_plane_api* app) noexcept
  : _rtc(_as)
  , _partition(std::move(p))
  , _data_plane(app)
  , _ctp_stm_api(make_ctp_stm_api(_rtc, _partition)) {}

const model::ntp& frontend::ntp() const { return _partition->ntp(); }

static kafka::offset get_log_end_offset(cluster::partition& p) {
    auto ot_state = p.get_offset_translator_state();
    // Local log is empty
    if (p.dirty_offset() < p.raft_start_offset()) {
        return model::offset_cast(
          ot_state->from_log_offset(p.raft_start_offset()));
    }
    // Local log is not empty
    return model::offset_cast(
      ot_state->from_log_offset(model::next_offset(p.dirty_offset())));
}

static ss::future<std::vector<cluster::tx::tx_range>>
get_aborted_transactions_local(
  cluster::partition& p, cloud_storage::offset_range offsets) {
    // The reconciled data should have aborted transactions removed.
    // This means that we should only read aborted transactions for
    // recent offsets which are not reconciled yet.

    auto ot_state = p.get_offset_translator_state();
    auto source = co_await p.aborted_transactions(
      offsets.begin_rp, offsets.end_rp);

    std::vector<cluster::tx::tx_range> target;
    target.reserve(source.size());
    for (const auto& range : source) {
        target.emplace_back(
          range.pid,
          ot_state->from_log_offset(range.first),
          ot_state->from_log_offset(range.last));
    }

    co_return target;
}

kafka::offset frontend::local_start_offset() const {
    // NOTE: the "local" start offset is only used by the datalake subsystem.
    // The method defines the boundary starting from which the translation
    // could be performed. In case of cloud topics there is no such boundary
    // because all data if fetched from the cloud storage. Therefore this method
    // is just an alias for the 'start_offset'.
    return start_offset();
}

kafka::offset frontend::start_offset() const {
    // Ask partition for its start offset
    // TODO: query metadata layer to get the actual start offset.
    // the 'partition::sync_kafka_start_offset_override' is not invoked here
    // because it's tied to both archival_metadata_stm and log_eviction_stm.
    // For cloud topics we will do log eviction differently and the
    // DeleteRecords API is not implemented yet. So the code is just
    // using the start_offset of the Raft log at the moment which is incorrect.
    auto so = _partition->raft_start_offset();
    auto kso = _partition->get_offset_translator_state()->from_log_offset(so);
    return model::offset_cast(kso);
}

ss::future<std::expected<kafka::offset, frontend_errc>>
frontend::sync_effective_start(model::timeout_clock::duration) {
    // TODO: ask metadata layer
    co_return start_offset();
}

kafka::offset frontend::high_watermark() const {
    auto ot_state = _partition->get_offset_translator_state();
    return model::offset_cast(
      ot_state->from_log_offset(_partition->high_watermark()));
}

std::expected<kafka::offset, frontend_errc>
frontend::last_stable_offset() const {
    auto maybe_lso = _partition->last_stable_offset();
    if (maybe_lso == model::invalid_lso) {
        return std::unexpected(frontend_errc::offset_not_available);
    }
    auto ot_state = _partition->get_offset_translator_state();
    return model::offset_cast(ot_state->from_log_offset(maybe_lso));
}

bool frontend::is_leader() const { return _partition->is_leader(); }

model::term_id frontend::leader_epoch() const {
    return _partition->raft()->confirmed_term();
}

ss::future<storage::translating_reader> frontend::make_reader(
  cloud_topic_log_reader_config cfg,
  std::optional<model::timeout_clock::time_point>) {
    vassert(_data_plane != nullptr, "cloud topics api not initialized");

    auto ot_state = _partition->get_offset_translator_state();

    // TODO: depending on the 'cfg' construct level zero or level one
    // reader impl.
    auto impl = std::make_unique<level_zero_log_reader_impl>(
      cfg, _partition, _data_plane);

    co_return storage::translating_reader{
      model::record_batch_reader(std::move(impl)), std::move(ot_state)};
}

ss::future<std::vector<cluster::tx::tx_range>> frontend::aborted_transactions(
  kafka::offset base,
  kafka::offset last,
  ss::lw_shared_ptr<const storage::offset_translator_state> ot_state) {
    auto base_rp = ot_state->to_log_offset(kafka::offset_cast(base));
    auto last_rp = ot_state->to_log_offset(kafka::offset_cast(last));
    cloud_storage::offset_range offsets = {
      .begin = base,
      .end = last,
      .begin_rp = base_rp,
      .end_rp = last_rp,
    };
    co_return co_await get_aborted_transactions_local(*_partition, offsets);
}

bool frontend::cache_enabled() const {
    if (!_partition->log()->config().cache_enabled()) {
        return false;
    }
    if (config::shard_local_cfg().disable_batch_cache()) {
        return false;
    }
    return true;
}

ss::future<std::optional<storage::timequery_result>>
frontend::timequery(storage::timequery_config cfg) {
    // cluster::partition::timequery returns a result in Kafka offsets,
    // no further offset translation is required here.
    // TODO: take metadata layer state into account
    return _partition->timequery(cfg);
}

namespace {

raft::replicate_options update_replicate_options(raft::replicate_options opts) {
    // We overwrite the consistency level in cloud topics. Since you're already
    // willing to wait for object storage uploads, it's much safer to make sure
    // metadata is written to a majority before acking the write as well. This
    // *should* be very little extra latency compared to writing to object
    // storage anyways.
    //
    // The primary motivation for this is that at the time of writing, lower
    // consistency levels *also* modify the offsets that are visible to
    // consumers - and to prevent situations where we have to suffix truncate
    // the log, we just force a majority to persist the write before responding.
    opts.consistency = raft::consistency_level::quorum_ack;
    return opts;
}

struct upload_and_replicate_stages {
    model::ntp ntp;
    ss::lw_shared_ptr<cluster::partition> partition;
    ss::lw_shared_ptr<cloud_topics::ctp_stm_api> ctp_stm_api;
    chunked_vector<model::record_batch> batches;
    model::batch_identity batch_id;
    raft::replicate_options opts;
    std::chrono::milliseconds timeout;

    upload_and_replicate_stages(
      ss::lw_shared_ptr<cluster::partition> partition,
      chunked_vector<model::record_batch> batches,
      model::batch_identity batch_id,
      raft::replicate_options opts,
      retry_chain_node& rtc,
      std::chrono::milliseconds timeout)
      : ntp(partition->ntp())
      , partition(std::move(partition))
      , ctp_stm_api(make_ctp_stm_api(rtc, this->partition))
      , batches(std::move(batches))
      , batch_id(batch_id)
      , opts(update_replicate_options(opts))
      , timeout(timeout) {}

    ss::promise<> request_enqueued;
    ss::promise<result<raft::replicate_result>> replicate_finished;
};

ss::future<> bg_upload_and_replicate(
  data_plane_api* api,
  ss::lw_shared_ptr<cluster::partition> partition,
  model::record_batch_header header,
  ss::lw_shared_ptr<upload_and_replicate_stages> op,
  bool cache_enabled) {
    vassert(api != nullptr, "cloud topics api is not initialized");

    auto fallback = ss::defer([op] {
        // This guarantees that the promises are set.
        // The error code used here does not represent the
        // actual error.
        op->request_enqueued.set_value();
        op->replicate_finished.set_value(raft::errc::timeout);
    });

    chunked_vector<model::record_batch> rb_copy;
    if (cache_enabled) {
        rb_copy = clone_batches(op->batches);
    }
    auto timeout = op->timeout == 0ms ? L0_upload_default_timeout : op->timeout;
    auto res = co_await api->write_and_debounce(
      op->ntp, std::move(op->batches), model::timeout_clock::now() + timeout);

    if (res.has_error()) {
        vlog(
          cd_log.debug,
          "LO object upload has failed: {}",
          res.error().message());
        co_return;
    }

    if (res.value().empty()) {
        vlog(
          cd_log.warn,
          "LO object upload returned empty result, nothing to replicate");
        co_return;
    }

    auto fence_fut = co_await ss::coroutine::as_future(
      op->ctp_stm_api->fence_epoch(res.value().front().id.epoch));
    if (fence_fut.failed()) {
        auto e = fence_fut.get_exception();
        vlog(
          cd_log.warn,
          "Failed to fence epoch {} for ntp {}, error: {}",
          res.value().front().id.epoch,
          op->ntp,
          e);
        co_return;
    }
    auto fence = std::move(fence_fut.get());
    if (!fence.unit.has_value()) {
        vlog(
          cd_log.warn,
          "Failed to fence epoch {} for ntp {}, fence unit is empty",
          res.value().front().id.epoch,
          op->ntp);
        co_return;
    }

    chunked_vector<model::record_batch_header> headers;
    headers.push_back(header);
    auto placeholders = convert_to_placeholders(
      res.value(), std::move(headers));

    vassert(
      placeholders.batches.size() == 1,
      "Expected single batch, got {}",
      placeholders.batches.size());

    // Replicate
    auto replicate_stages = partition->replicate_in_stages(
      op->batch_id, std::move(placeholders.batches.front()), op->opts);

    fallback.cancel();

    // Forward future result to the 'op'. The expectation is that at this point
    // the target promises (inside 'op') are used to generate futures and these
    // futures are awaited.
    replicate_stages.request_enqueued.forward_to(
      std::move(op->request_enqueued));

    auto replicate_fut
      = std::move(replicate_stages.replicate_finished)
          .then(
            [api,
             cache_enabled,
             inp = std::move(rb_copy),
             ntp = partition->ntp(),
             fence_unit = std::move(fence.unit)](
              result<cluster::kafka_result> res) mutable
              -> result<raft::replicate_result> {
                if (res.has_error()) {
                    return res.error();
                }
                if (cache_enabled) {
                    // The term_id is not guaranteed to be set if the request
                    // was served from the list of finished requests. This might
                    // happen if the request is coming from the snapshot (in
                    // which case it's not stored) or from the log replay. The
                    // simplest solution in this case is to skip caching.
                    if (res.value().last_term >= model::term_id{0}) {
                        update_batches(
                          inp,
                          kafka::offset_cast(res.value().last_offset),
                          res.value().last_term);
                        for (const auto& b : inp) {
                            vlog(
                              cd_log.trace,
                              "Putting batch to cache: {}, term: {}",
                              b.base_offset(),
                              b.term());
                            api->cache_put(ntp, b);
                        }
                    } else {
                        vlog(
                          cd_log.debug,
                          "Skipping cache put for ntp {} at offset {} with "
                          "unset term",
                          ntp,
                          res.value().last_offset);
                    }
                }
                return raft::replicate_result{
                  .last_offset = kafka::offset_cast(res.value().last_offset),
                  .last_term = res.value().last_term,
                };
            });

    replicate_fut.forward_to(std::move(op->replicate_finished));
}
} // namespace

ss::future<std::expected<kafka::offset, std::error_code>> frontend::replicate(
  chunked_vector<model::record_batch> batches, raft::replicate_options opts) {
    opts = update_replicate_options(opts);
    chunked_vector<model::record_batch_header> headers;
    headers.reserve(batches.size());
    for (const auto& batch : batches) {
        headers.push_back(batch.header());
    }

    chunked_vector<model::record_batch> rb_copy;
    if (cache_enabled()) {
        rb_copy = clone_batches(batches);
    }

    // Dataplane.
    auto res = co_await _data_plane->write_and_debounce(
      ntp(),
      std::move(batches),
      model::timeout_clock::now()
        + opts.timeout.value_or(L0_replicate_default_timeout));

    if (res.has_error()) {
        co_return std::unexpected(res.error());
    }

    auto fence_fut = co_await ss::coroutine::as_future(
      _ctp_stm_api->fence_epoch(res.value().front().id.epoch));
    if (fence_fut.failed()) {
        // TODO: handle shutdown failures gracefully
        auto e = fence_fut.get_exception();
        vlog(
          cd_log.warn,
          "Failed to fence epoch {} for ntp {}, error: {}",
          res.value().front().id.epoch,
          ntp(),
          fence_fut.get_exception());
        std::rethrow_exception(e);
    }
    auto fence = std::move(fence_fut.get());
    if (!fence.unit.has_value()) {
        vlog(
          cd_log.warn,
          "Failed to fence epoch {} for ntp {}, fence unit is empty",
          res.value().front().id.epoch,
          ntp());

        /// TODO: Maybe return different error code here?
        co_return std::unexpected(
          kafka::make_error_code(kafka::error_code::request_timed_out));
    }

    auto placeholders = convert_to_placeholders(res.value(), headers);

    chunked_vector<model::record_batch> placeholder_batches;
    for (auto&& batch : placeholders.batches) {
        placeholder_batches.push_back(std::move(batch));
    }

    auto result = co_await _partition->replicate(
      std::move(placeholder_batches), opts);

    if (!result) {
        co_return std::unexpected(result.error());
    }
    auto ret_offset = model::offset(result.value().last_offset());
    if (!rb_copy.empty()) {
        update_batches(rb_copy, ret_offset, result.value().last_term);
        for (const auto& b : rb_copy) {
            vlog(
              cd_log.trace,
              "Putting batch for {} to cache: {}, term: {}",
              ntp(),
              b.base_offset(),
              b.term());
            _data_plane->cache_put(ntp(), b);
        }
    }
    co_return ret_offset;
}

raft::replicate_stages frontend::replicate(
  model::batch_identity batch_id,
  model::record_batch batch,
  raft::replicate_options opts) {
    auto header = batch.header();
    chunked_vector<model::record_batch> batch_vec;
    batch_vec.push_back(std::move(batch));
    auto op_state = ss::make_lw_shared<upload_and_replicate_stages>(
      _partition,
      std::move(batch_vec),
      batch_id,
      opts,
      _rtc,
      opts.timeout.value_or(L0_replicate_default_timeout));

    raft::replicate_stages out(raft::errc::success);
    out.request_enqueued = op_state->request_enqueued.get_future();
    out.replicate_finished = op_state->replicate_finished.get_future();
    ssx::background = bg_upload_and_replicate(
      _data_plane, _partition, header, op_state, cache_enabled());
    return out;
}

ss::future<std::optional<kafka::offset>>
frontend::get_leader_epoch_last_offset(model::term_id term) const {
    auto ot_state = _partition->get_offset_translator_state();
    auto first_local_offset = _partition->raft_start_offset();
    auto first_local_term = _partition->get_term(first_local_offset);
    auto last_local_term = _partition->term();

    if (term > last_local_term) {
        co_return std::nullopt;
    }

    if (term >= first_local_term) {
        auto last_offset = _partition->get_term_last_offset(term);
        if (last_offset) {
            co_return ot_state->from_log_offset(*last_offset);
        }
    }

    co_return start_offset();
}

ss::future<std::expected<void, frontend_errc>>
frontend::prefix_truncate(kafka::offset, ss::lowres_clock::time_point) {
    /// DeleteRecords API is not supported in cloud topics yet.
    co_return std::unexpected(frontend_errc::invalid_topic_exception);
}

ss::future<std::expected<std::monostate, frontend_errc>>
frontend::validate_fetch_offset(
  kafka::offset fetch_offset,
  bool reading_from_follower,
  model::timeout_clock::time_point deadline) {
    if (reading_from_follower && !_partition->is_leader()) {
        std::optional<frontend_errc> ec = std::nullopt;
        auto log_end_offset = get_log_end_offset(*_partition);

        model::offset leader_hwm;
        kafka::offset available_to_read;

        if (!ec.has_value()) {
            leader_hwm
              = _partition->get_offset_translator_state()->from_log_offset(
                _partition->leader_high_watermark());
            available_to_read = std::min(
              model::offset_cast(leader_hwm), log_end_offset);

            if (fetch_offset < start_offset()) {
                ec = frontend_errc::offset_out_of_range;
            } else if (fetch_offset > available_to_read) {
                // Offset know to be committed but not yet available on the
                // follower.
                ec = frontend_errc::offset_not_available;
            }
        }

        if (ec.has_value()) {
            vlog(
              cd_log.warn,
              "ntp {}: fetch offset out of range on follower, requested: {}, "
              "partition start offset: {}, high watermark: {}, leader high "
              "watermark: {}, log end offset: {}, ec: {}",
              ntp(),
              fetch_offset,
              start_offset(),
              high_watermark(),
              leader_hwm,
              log_end_offset,
              ec);
            co_return std::unexpected(*ec);
        }
        co_return std::monostate{};
    }

    auto timeout = deadline - model::timeout_clock::now();
    auto so = co_await sync_effective_start(timeout);
    if (!so) {
        co_return std::unexpected(so.error());
    }

    if (
      fetch_offset < so.value()
      || fetch_offset > get_log_end_offset(*_partition)) {
        co_return std::unexpected(frontend_errc::offset_out_of_range);
    }

    co_return std::monostate{};
}

std::expected<partition_info, frontend_errc>
frontend::get_partition_info() const {
    auto ot_state = _partition->get_offset_translator_state();
    partition_info ret;
    ret.leader = _partition->get_leader_id();
    ret.replicas.reserve(_partition->raft()->get_follower_count() + 1);
    auto followers = _partition->get_follower_metrics();

    if (followers.has_error()) {
        return std::unexpected(frontend_errc::not_leader_for_partition);
    }
    auto start_offset = _partition->raft_start_offset();

    auto clamped_translate = [ot_state,
                              start_offset](model::offset to_translate) {
        return model::offset_cast(
          to_translate >= start_offset
            ? ot_state->from_log_offset(to_translate)
            : ot_state->from_log_offset(start_offset));
    };

    for (const auto& follower_metric : followers.value()) {
        ret.replicas.push_back(
          replica_info{
            .id = follower_metric.id,
            .high_watermark = kafka::next_offset(
              clamped_translate(follower_metric.match_index)),
            .log_end_offset = kafka::next_offset(
              clamped_translate(follower_metric.dirty_log_index)),
            .is_alive = follower_metric.is_live,
          });
    }

    ret.replicas.push_back(
      replica_info{
        .id = _partition->raft()->self().id(),
        .high_watermark = high_watermark(),
        .log_end_offset = get_log_end_offset(*_partition),
        .is_alive = true,
      });

    return {std::move(ret)};
}

size_t frontend::estimate_size_between(kafka::offset, kafka::offset) const {
    // TODO: implement this function
    // This function can't be implemented yet because the L1 read path is not
    // completely implemented.
    return 0;
}

ss::future<std::error_code> frontend::linearizable_barrier() {
    // TODO: implement linearizable barrier for cloud topics
    auto r = co_await _partition->linearizable_barrier();
    if (r) {
        co_return raft::errc::success;
    }
    co_return r.error();
}

} // namespace cloud_topics
