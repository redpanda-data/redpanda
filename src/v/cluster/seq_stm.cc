// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/seq_stm.h"

#include "cluster/logger.h"
#include "raft/errc.h"
#include "raft/types.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

#include <filesystem>

namespace cluster {

seq_stm::seq_stm(ss::logger& logger, raft::consensus* c)
  : persisted_stm("seq", logger, c)
  , _oldest_session(model::timestamp::now())
  , _transactional_id_expiration(
      config::shard_local_cfg().transactional_id_expiration_ms.value()) {}

void seq_stm::load_snapshot(stm_snapshot_header hdr, iobuf&& data_buf) {
    vassert(
      hdr.version == seq_snapshot_version,
      "unsupported seq_snapshot_header version {} expected: {}",
      hdr.version,
      seq_snapshot_version);
    iobuf_parser data_parser(std::move(data_buf));
    auto data = reflection::adl<seq_snapshot>{}.from(data_parser);
    for (auto& entry : data.entries) {
        auto [seq_it, _] = _seq_table.try_emplace(entry.pid, entry);
        if (seq_it->second.seq < entry.seq) {
            seq_it->second = entry;
        }
    }
    _last_snapshot_offset = data.offset;
    _insync_offset = data.offset;
    vlog(
      clusterlog.trace,
      "seq_stm snapshot with offset:{} is loaded",
      data.offset);
}

stm_snapshot seq_stm::take_snapshot() {
    seq_snapshot data;
    data.offset = _insync_offset;
    for (auto& entry : _seq_table) {
        data.entries.push_back(entry.second);
    }

    iobuf buf;
    reflection::adl<seq_snapshot>{}.to(buf, data);

    stm_snapshot_header header;
    header.version = seq_snapshot_version;
    header.snapshot_size = buf.size_bytes();

    stm_snapshot stm_ss;
    stm_ss.header = header;
    stm_ss.offset = _insync_offset;
    stm_ss.data = std::move(buf);
    return stm_ss;
}

static bool is_sequence(int32_t last_seq, int32_t next_seq) {
    return (last_seq + 1 == next_seq)
           || (next_seq == 0 && last_seq == std::numeric_limits<int32_t>::max());
}

// https://github.com/vectorizedio/redpanda/issues/841
ss::future<checked<raft::replicate_result, kafka::error_code>>
seq_stm::replicate(
  model::batch_identity bid,
  model::record_batch_reader br,
  raft::replicate_options opts) {
    if (bid.has_idempotent() && bid.first_seq <= bid.last_seq) {
        auto timeout = config::shard_local_cfg().seq_sync_timeout_ms.value();
        auto is_ready = co_await sync(timeout);
        if (!is_ready) {
            co_return checked<raft::replicate_result, kafka::error_code>(
              kafka::error_code::not_leader_for_partition);
        }
        auto pid_seq = _seq_table.find(bid.pid);
        auto last_write_timestamp = model::timestamp::now().value();
        if (pid_seq == _seq_table.end()) {
            if (bid.first_seq != 0) {
                co_return checked<raft::replicate_result, kafka::error_code>(
                  kafka::error_code::out_of_order_sequence_number);
            }
            seq_entry entry{
              .pid = bid.pid,
              .seq = bid.last_seq,
              .last_write_timestamp = last_write_timestamp};
            _oldest_session = std::min(
              _oldest_session, model::timestamp(entry.last_write_timestamp));
            _seq_table.emplace(entry.pid, entry);
        } else {
            if (!is_sequence(pid_seq->second.seq, bid.first_seq)) {
                co_return checked<raft::replicate_result, kafka::error_code>(
                  kafka::error_code::out_of_order_sequence_number);
            }
            pid_seq->second.seq = bid.last_seq;
            pid_seq->second.last_write_timestamp = last_write_timestamp;
            _oldest_session = std::min(
              _oldest_session,
              model::timestamp(pid_seq->second.last_write_timestamp));
        }
        auto r = co_await _c->replicate(_insync_term, std::move(br), opts);
        if (r) {
            co_return checked<raft::replicate_result, kafka::error_code>(
              r.value());
        }
        co_return checked<raft::replicate_result, kafka::error_code>(
          kafka::error_code::unknown_server_error);
    }
    auto r = co_await _c->replicate(std::move(br), std::move(opts));
    if (r) {
        co_return checked<raft::replicate_result, kafka::error_code>(r.value());
    }
    co_return checked<raft::replicate_result, kafka::error_code>(
      kafka::error_code::unknown_server_error);
}

void seq_stm::compact_snapshot() {
    auto cutoff_timestamp = model::timestamp::now().value()
                            - _transactional_id_expiration.count();
    if (_oldest_session.value() < cutoff_timestamp) {
        auto next_oldest_session = model::timestamp::now();
        for (auto it = _seq_table.cbegin(); it != _seq_table.cend();) {
            if (it->second.last_write_timestamp < cutoff_timestamp) {
                _seq_table.erase(it++);
            } else {
                next_oldest_session = std::min(
                  next_oldest_session,
                  model::timestamp(it->second.last_write_timestamp));
                ++it;
            }
        }
        _oldest_session = next_oldest_session;
    }
}

ss::future<> seq_stm::apply(model::record_batch b) {
    const auto& hdr = b.header();
    auto bid = model::batch_identity::from(hdr);

    if (
      hdr.type == raft::data_batch_type && bid.has_idempotent()
      && bid.first_seq <= bid.last_seq) {
        auto pid_seq = _seq_table.find(bid.pid);
        if (pid_seq == _seq_table.end()) {
            seq_entry entry{
              .pid = bid.pid,
              .seq = bid.last_seq,
              .last_write_timestamp = hdr.max_timestamp.value()};
            _seq_table.emplace(bid.pid, entry);
            _oldest_session = std::min(
              _oldest_session, model::timestamp(entry.last_write_timestamp));
        } else if (pid_seq->second.seq < bid.last_seq) {
            pid_seq->second.seq = bid.last_seq;
            pid_seq->second.last_write_timestamp = hdr.max_timestamp.value();
            _oldest_session = std::min(_oldest_session, hdr.max_timestamp);
        }
        compact_snapshot();
    }

    _insync_offset = b.last_offset();

    return ss::now();
}

} // namespace cluster
