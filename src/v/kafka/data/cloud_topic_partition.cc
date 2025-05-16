/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "kafka/data/cloud_topic_partition.h"

#include "cloud_storage/types.h"
#include "cloud_topics/app.h"
#include "cluster/partition.h"
#include "cluster/rm_stm.h"
#include "cluster/types.h"
#include "kafka/protocol/batch_reader.h"
#include "kafka/protocol/errors.h"
#include "logger.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "raft/consensus_utils.h"
#include "raft/errc.h"
#include "raft/replicate.h"
#include "storage/log_reader.h"
#include "storage/types.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/coroutine/as_future.hh>

#include <chrono>
#include <optional>
#include <system_error>

namespace kafka {
cloud_topic_partition::cloud_topic_partition(
  ss::lw_shared_ptr<cluster::partition> p,
  ss::shared_ptr<experimental::cloud_topics::api> app) noexcept
  : _partition(std::move(p))
  , _ct_api(std::move(app)) {}

cloud_topic_partition::cloud_topic_partition(
  ss::lw_shared_ptr<cluster::partition> p,
  ss::sharded<experimental::cloud_topics::app>& ct_app) noexcept
  : cloud_topic_partition(std::move(p), ct_app.local().get_api()) {}

const model::ntp& cloud_topic_partition::ntp() const {
    return _partition->ntp();
}

static model::offset get_log_end_offset(cluster::partition& p) {
    auto ot_state = p.get_offset_translator_state();
    // Local log is empty
    if (p.dirty_offset() < p.raft_start_offset()) {
        return ot_state->from_log_offset(p.raft_start_offset());
    }
    // Local log is not empty
    return ot_state->from_log_offset(model::next_offset(p.dirty_offset()));
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

model::offset cloud_topic_partition::local_start_offset() const {
    auto ot_state = _partition->get_offset_translator_state();
    return ot_state->from_log_offset(_partition->raft_start_offset());
}

model::offset cloud_topic_partition::start_offset() const {
    // Ask partition for its start offset
    // TODO: ask dl_stm for its start offset
    // the 'partition::sync_kafka_start_offset_override' is not invoked here
    // because it's tied to both archival_metadata_stm and log_eviction_stm.
    // For cloud topics we will do log eviction differently and the
    // DeleteRecords API is not implemented yet. So the code is just
    // using the start_offset of the Raft log at the moment which is incorrect.
    auto so = _partition->raft_start_offset();
    auto kso = _partition->get_offset_translator_state()->from_log_offset(so);
    return kso;
}

ss::future<result<model::offset, error_code>>
cloud_topic_partition::sync_effective_start(model::timeout_clock::duration) {
    // TODO: ask dl_stm
    co_return start_offset();
}

model::offset cloud_topic_partition::high_watermark() const {
    auto ot_state = _partition->get_offset_translator_state();
    return ot_state->from_log_offset(_partition->high_watermark());
}

checked<model::offset, error_code>
cloud_topic_partition::last_stable_offset() const {
    auto maybe_lso = _partition->last_stable_offset();
    if (maybe_lso == model::invalid_lso) {
        return error_code::offset_not_available;
    }
    auto ot_state = _partition->get_offset_translator_state();
    return ot_state->from_log_offset(maybe_lso);
}

bool cloud_topic_partition::is_leader() const {
    return _partition->is_leader();
}

ss::future<std::error_code> cloud_topic_partition::linearizable_barrier() {
    auto r = co_await _partition->linearizable_barrier();
    if (r) {
        co_return raft::errc::success;
    }
    co_return r.error();
}

cluster::partition_probe& cloud_topic_partition::probe() {
    return _partition->probe();
}

kafka::leader_epoch cloud_topic_partition::leader_epoch() const {
    return leader_epoch_from_term(_partition->raft()->confirmed_term());
}

ss::future<storage::translating_reader> cloud_topic_partition::make_reader(
  storage::log_reader_config cfg,
  std::optional<model::timeout_clock::time_point> timeout) {
    vassert(_ct_api != nullptr, "cloud topics api not initialized");

    auto ot_state = _partition->get_offset_translator_state();

    cfg.start_offset = ot_state->to_log_offset(cfg.start_offset);
    cfg.max_offset = ot_state->to_log_offset(cfg.max_offset);
    cfg.translate_offsets = storage::translate_offsets::yes;
    cfg.type_filter = {model::record_batch_type::dl_placeholder};

    // TODO: add code path that fetches metadata from the dl_stm for L1 read
    // path

    // This part comprises the control plane query. The partition is a metadata
    // storage for the cloud topic.
    auto underlying = co_await _partition->make_reader(cfg, timeout);
    auto placeholders = co_await model::consume_reader_to_memory(
      std::move(underlying), model::no_timeout);

    // This part comprises the data plane query. The 'cloud_topics::api' is
    // responsible for moving the data from the cloud storage to the local
    // cache.
    auto data_batches = co_await _ct_api->materialize(
      ntp(),
      cfg.max_bytes,
      std::move(placeholders),
      // TODO: use configurable default timeout or derive from the log reader
      // config
      10s);

    if (!data_batches) {
        throw std::system_error(data_batches.error());
    }

    co_return storage::translating_reader(
      model::make_memory_record_batch_reader(std::move(data_batches.value())),
      ot_state);
}

ss::future<std::vector<cluster::tx::tx_range>>
cloud_topic_partition::aborted_transactions(
  model::offset base,
  model::offset last,
  ss::lw_shared_ptr<const storage::offset_translator_state> ot_state) {
    // TODO: add code path that fetches metadata from the dl_stm for L1 read
    auto base_rp = ot_state->to_log_offset(base);
    auto last_rp = ot_state->to_log_offset(last);
    cloud_storage::offset_range offsets = {
      .begin = model::offset_cast(base),
      .end = model::offset_cast(last),
      .begin_rp = base_rp,
      .end_rp = last_rp,
    };
    co_return co_await get_aborted_transactions_local(*_partition, offsets);
}

ss::future<std::optional<storage::timequery_result>>
cloud_topic_partition::timequery(storage::timequery_config cfg) {
    // cluster::partition::timequery returns a result in Kafka offsets,
    // no further offset translation is required here.
    // TODO: take dl_stm state into account
    return _partition->timequery(cfg);
}

// Consume the entire reader
struct upload_and_replicate_stages {
    model::ntp ntp;
    ss::lw_shared_ptr<cluster::partition> partition;
    model::record_batch_reader reader;
    model::batch_identity batch_id;
    raft::replicate_options opts;
    std::chrono::milliseconds timeout;

    upload_and_replicate_stages(
      ss::lw_shared_ptr<cluster::partition> partition,
      model::record_batch_reader reader,
      model::batch_identity batch_id,
      raft::replicate_options opts,
      std::chrono::milliseconds timeout)
      : ntp(partition->ntp())
      , partition(std::move(partition))
      , reader(std::move(reader))
      , batch_id(batch_id)
      , opts(opts)
      , timeout(timeout) {}

    ss::promise<> request_enqueued;
    ss::promise<result<raft::replicate_result>> replicate_finished;
};

static ss::future<> bg_upload_and_replicate(
  ss::shared_ptr<experimental::cloud_topics::api> api,
  ss::lw_shared_ptr<cluster::partition> partition,
  ss::lw_shared_ptr<upload_and_replicate_stages> op) {
    vassert(api != nullptr, "cloud topics api is not initialized");
    auto fallback = ss::defer([op] {
        // This guarantees that the promises are set.
        // The error code used here does not represent the
        // actual error.
        op->request_enqueued.set_value();
        op->replicate_finished.set_value(raft::errc::timeout);
    });
    auto timeout = op->timeout == 0ms ? 1s : op->timeout;
    auto pl = co_await api->write_and_debounce(
      op->ntp, std::move(op->reader), timeout);

    if (pl.has_error()) {
        vlog(
          kdlog.debug, "LO object upload has failed: {}", pl.error().message());
        co_return;
    }

    // Unpack record_batch_reader (TODO: get rid of
    // record_batch_reader)
    auto placeholder_batches = std::move(pl.value());

    vassert(
      placeholder_batches.size() == 1,
      "Expected single batch, got {}",
      placeholder_batches.size());

    // Replicate
    auto replicate_stages = partition->replicate_in_stages(
      op->batch_id, std::move(placeholder_batches.front()), op->opts);

    fallback.cancel();

    // Forward future result to the 'op'. The expectation is that at this point
    // the target promises (inside 'op') are used to generate futures and these
    // futures are awaited.
    replicate_stages.request_enqueued.forward_to(
      std::move(op->request_enqueued));

    auto replicate_fut = std::move(replicate_stages.replicate_finished)
                           .then(
                             [](result<cluster::kafka_result> res)
                               -> result<raft::replicate_result> {
                                 if (res.has_error()) {
                                     return res.error();
                                 }
                                 return raft::replicate_result{
                                   model::offset(res.value().last_offset())};
                             });

    replicate_fut.forward_to(std::move(op->replicate_finished));
}

ss::future<result<model::offset>> cloud_topic_partition::replicate(
  chunked_vector<model::record_batch> batches, raft::replicate_options opts) {
    using ret_t = result<model::offset>;
    auto batch_reader = model::make_fragmented_memory_record_batch_reader(
      std::move(batches));
    // TODO: use a config
    auto default_timeout = 1s;

    // Dataplane.
    auto placeholders = co_await _ct_api->write_and_debounce(
      ntp(), std::move(batch_reader), opts.timeout.value_or(default_timeout));

    if (placeholders.has_error()) {
        co_return ret_t(placeholders.error());
    }

    // TODO: change the return type of write_and_debounce to
    // avoid type conversion.
    chunked_vector<model::record_batch> placeholder_batches;
    for (auto&& batch : placeholders.value()) {
        placeholder_batches.push_back(std::move(batch));
    }

    // Control plane.
    auto result = co_await _partition->replicate(
      std::move(placeholder_batches), opts);
    if (!result) {
        co_return ret_t(result.error());
    }
    co_return ret_t(model::offset(result.value().last_offset()));
}

ss::future<result<model::offset>> cloud_topic_partition::replicate(
  model::record_batch batch, raft::replicate_options opts) {
    return replicate(
      chunked_vector<model::record_batch>::single(std::move(batch)), opts);
}

raft::replicate_stages cloud_topic_partition::replicate(
  model::batch_identity batch_id,
  model::record_batch batch,
  raft::replicate_options opts) {
    auto op_state = ss::make_lw_shared<upload_and_replicate_stages>(
      _partition,
      model::make_memory_record_batch_reader(std::move(batch)),
      batch_id,
      opts,
      opts.timeout.value_or(1s));

    raft::replicate_stages out(raft::errc::success);
    out.request_enqueued = op_state->request_enqueued.get_future();
    out.replicate_finished = op_state->replicate_finished.get_future();
    ssx::background = bg_upload_and_replicate(_ct_api, _partition, op_state);
    return out;
}

ss::future<std::optional<model::offset>>
cloud_topic_partition::get_leader_epoch_last_offset(
  kafka::leader_epoch epoch) const {
    auto ot_state = _partition->get_offset_translator_state();
    model::term_id term(epoch);
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

ss::future<error_code> cloud_topic_partition::prefix_truncate(
  model::offset, ss::lowres_clock::time_point) {
    /// DeleteRecords API is not supported in cloud topics yet.
    co_return error_code::invalid_topic_exception;
}

ss::future<error_code> cloud_topic_partition::validate_fetch_offset(
  model::offset fetch_offset,
  bool reading_from_follower,
  model::timeout_clock::time_point deadline) {
    if (reading_from_follower && !_partition->is_leader()) {
        // TODO: implement follower fetching for cloud topics
        co_return error_code::not_leader_for_partition;
    }

    auto timeout = deadline - model::timeout_clock::now();
    auto so = co_await sync_effective_start(timeout);
    if (!so) {
        co_return so.error();
    }

    if (
      fetch_offset < so.value()
      || fetch_offset > get_log_end_offset(*_partition)) {
        co_return error_code::offset_out_of_range;
    }

    co_return error_code::none;
}

result<partition_info> cloud_topic_partition::get_partition_info() const {
    auto ot_state = _partition->get_offset_translator_state();
    partition_info ret;
    ret.leader = _partition->get_leader_id();
    ret.replicas.reserve(_partition->raft()->get_follower_count() + 1);
    auto followers = _partition->get_follower_metrics();

    if (followers.has_error()) {
        return followers.error();
    }
    auto start_offset = _partition->raft_start_offset();

    auto clamped_translate = [ot_state,
                              start_offset](model::offset to_translate) {
        return to_translate >= start_offset
                 ? ot_state->from_log_offset(to_translate)
                 : ot_state->from_log_offset(start_offset);
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
      .log_end_offset = get_log_end_offset(*_partition),
      .is_alive = true,
    });

    return {std::move(ret)};
}

size_t cloud_topic_partition::estimate_size_between(
  kafka::offset, kafka::offset) const {
    // TODO: implement this function
    // This function can't be implemented yet because the L1 read path is not
    // completely implemented.
    return 0;
}

} // namespace kafka
