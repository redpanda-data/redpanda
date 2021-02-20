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

#include <seastar/core/future.hh>

#include <filesystem>

namespace cluster {

seq_stm::seq_stm(
  ss::logger& logger, raft::consensus* c, config::configuration& config)
  : raft::state_machine(c, logger, ss::default_priority_class())
  , _c(c)
  , _snapshot_mgr(
      std::filesystem::path(c->log_config().work_directory()),
      "seq",
      ss::default_priority_class())
  , _log(logger)
  , _config(config) {}

ss::future<> seq_stm::hydrate_snapshot(storage::snapshot_reader& reader) {
    return reader.read_metadata().then([this, &reader](iobuf meta_buf) {
        iobuf_parser meta_parser(std::move(meta_buf));
        seq_snapshot_header hdr;
        hdr.version = reflection::adl<int8_t>{}.from(meta_parser);
        vassert(
          hdr.version == seq_snapshot_header::supported_version,
          "unsupported seq_snapshot_header version {}",
          hdr.version);
        hdr.snapshot_size = reflection::adl<int32_t>{}.from(meta_parser);

        return read_iobuf_exactly(reader.input(), hdr.snapshot_size)
          .then([this](iobuf data_buf) {
              iobuf_parser data_parser(std::move(data_buf));
              auto data = reflection::adl<snapshot>{}.from(data_parser);
              for (auto& entry : data.entries) {
                  auto pid_seq = _seq_table.find(entry.pid);
                  if (pid_seq == _seq_table.end()) {
                      _seq_table.emplace(entry.pid, entry);
                  } else if (pid_seq->second.seq < entry.seq) {
                      pid_seq->second = entry;
                  }
              }
              _last_snapshot_offset = data.offset;
              _insync_offset = data.offset;
              vlog(
                clusterlog.trace,
                "seq_stm snapshot with offset:{} is loaded",
                data.offset);
          })
          .then([this]() { return _snapshot_mgr.remove_partial_snapshots(); });
    });
}

ss::future<> seq_stm::wait_for_snapshot_hydrated() {
    auto f = ss::now();
    if (unlikely(!_resolved_when_snapshot_hydrated.available())) {
        f = _resolved_when_snapshot_hydrated.get_shared_future();
    }
    return f;
}

ss::future<> seq_stm::persist_snapshot(iobuf&& data) {
    seq_snapshot_header header;
    header.version = seq_snapshot_header::supported_version;
    header.snapshot_size = data.size_bytes();

    iobuf data_size_buf;
    reflection::serialize(data_size_buf, header.version, header.snapshot_size);

    return _snapshot_mgr.start_snapshot().then(
      [this, data = std::move(data), data_size_buf = std::move(data_size_buf)](
        storage::snapshot_writer writer) mutable {
          return ss::do_with(
            std::move(writer),
            [this,
             data = std::move(data),
             data_size_buf = std::move(data_size_buf)](
              storage::snapshot_writer& writer) mutable {
                return writer.write_metadata(std::move(data_size_buf))
                  .then([&writer, data = std::move(data)]() mutable {
                      return write_iobuf_to_output_stream(
                        std::move(data), writer.output());
                  })
                  .finally([&writer] { return writer.close(); })
                  .then([this, &writer] {
                      return _snapshot_mgr.finish_snapshot(writer);
                  });
            });
      });
}

ss::future<> seq_stm::make_snapshot() {
    return _op_lock.with([this]() {
        auto f = wait_for_snapshot_hydrated();
        return f.then([this] { return do_make_snapshot(); });
    });
}

ss::future<> seq_stm::ensure_snapshot_exists(model::offset target_offset) {
    return _op_lock.with([this, target_offset]() {
        auto f = wait_for_snapshot_hydrated();

        return f.then([this, target_offset] {
            if (target_offset <= _last_snapshot_offset) {
                return ss::now();
            }
            return wait(target_offset, model::no_timeout)
              .then([this, target_offset]() {
                  vassert(
                    target_offset <= _insync_offset,
                    "after we waited for target_offset ({}) _insync_offset "
                    "({}) should have matched it or bypassed",
                    target_offset,
                    _insync_offset);
                  return do_make_snapshot();
              });
        });
    });
}

ss::future<> seq_stm::do_make_snapshot() {
    vlog(clusterlog.trace, "saving seq_stm snapshot");
    snapshot data;
    data.offset = _insync_offset;
    for (auto& entry : _seq_table) {
        data.entries.push_back(entry.second);
    }

    iobuf v_buf;
    reflection::adl<snapshot>{}.to(v_buf, data);
    return persist_snapshot(std::move(v_buf))
      .then([this, snapshot_offset = data.offset]() {
          if (snapshot_offset >= _last_snapshot_offset) {
              _last_snapshot_offset = snapshot_offset;
          }
      });
}

ss::future<> seq_stm::start() {
    _oldest_session = model::timestamp::now();
    return _snapshot_mgr.open_snapshot().then(
      [this](std::optional<storage::snapshot_reader> reader) {
          auto f = ss::now();
          if (reader) {
              f = ss::do_with(
                std::move(*reader), [this](storage::snapshot_reader& reader) {
                    return hydrate_snapshot(reader).finally([this, &reader] {
                        auto offset = std::max(
                          _insync_offset, _c->start_offset());
                        if (offset >= model::offset(0)) {
                            set_next(offset);
                        }
                        _resolved_when_snapshot_hydrated.set_value();
                        return reader.close();
                    });
                });
          } else {
              auto offset = _c->start_offset();
              if (offset >= model::offset(0)) {
                  set_next(offset);
              }
              _resolved_when_snapshot_hydrated.set_value();
          }

          return f.then([this]() { return state_machine::start(); })
            .then([this]() {
                auto offset = _c->meta().commit_index;
                if (offset >= model::offset(0)) {
                    (void)ss::with_gate(_gate, [this, offset] {
                        // saving a snapshot after catchup with the tip of the
                        // log
                        return ensure_snapshot_exists(offset);
                    });
                }
            });
      });
}

ss::future<> seq_stm::catchup() {
    if (_insync_term != _c->term()) {
        if (!_is_catching_up) {
            _is_catching_up = true;
            auto meta = _c->meta();
            return catchup(meta.prev_log_term, meta.prev_log_index)
              .then([this]() { _is_catching_up = false; });
        }
    }
    return ss::now();
}

static bool is_sequence(int32_t last_seq, int32_t next_seq) {
    return (last_seq + 1 == next_seq)
           || (next_seq == 0 && last_seq == std::numeric_limits<int32_t>::max());
}

ss::future<checked<raft::replicate_result, kafka::error_code>>
seq_stm::replicate(
  model::batch_identity bid,
  model::record_batch_reader&& r,
  raft::replicate_options opts) {
    if (bid.has_idempotent() && bid.first_seq <= bid.last_seq) {
        if (_insync_term != _c->term()) {
            (void)ss::with_gate(_gate, [this] { return catchup(); });
            return seastar::make_ready_future<
              checked<raft::replicate_result, kafka::error_code>>(
              kafka::error_code::not_leader_for_partition);
        }
        auto pid_seq = _seq_table.find(bid.pid);
        auto last_write_timestamp = model::timestamp::now().value();
        if (pid_seq == _seq_table.end()) {
            if (bid.first_seq != 0) {
                return seastar::make_ready_future<
                  checked<raft::replicate_result, kafka::error_code>>(
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
                return seastar::make_ready_future<
                  checked<raft::replicate_result, kafka::error_code>>(
                  kafka::error_code::out_of_order_sequence_number);
            }
            pid_seq->second.seq += bid.last_seq;
            pid_seq->second.last_write_timestamp = last_write_timestamp;
            _oldest_session = std::min(
              _oldest_session,
              model::timestamp(pid_seq->second.last_write_timestamp));
        }
        return _c->replicate(_insync_term, std::move(r), opts)
          .then([](result<raft::replicate_result> r) {
              if (r) {
                  return checked<raft::replicate_result, kafka::error_code>(
                    r.value());
              }
              return checked<raft::replicate_result, kafka::error_code>(
                kafka::error_code::unknown_server_error);
          });
    }
    return _c->replicate(std::move(r), std::move(opts))
      .then([](result<raft::replicate_result> r) {
          if (r) {
              return checked<raft::replicate_result, kafka::error_code>(
                r.value());
          }
          return checked<raft::replicate_result, kafka::error_code>(
            kafka::error_code::unknown_server_error);
      });
}

ss::future<>
seq_stm::catchup(model::term_id last_term, model::offset last_offset) {
    vlog(
      _log.trace,
      "seq_stm is catching up with term:{} and offset:{}",
      last_term,
      last_offset);
    return wait(last_offset, model::no_timeout)
      .then([this, last_offset, last_term]() {
          auto meta = _c->meta();
          vlog(
            _log.trace,
            "seq_stm caught up with term:{} and offset:{}; current_term: {} "
            "current_offset: {} dirty_term: {} dirty_offset: {}",
            last_term,
            last_offset,
            meta.term,
            meta.commit_index,
            meta.prev_log_term,
            meta.prev_log_index);
          if (last_term <= meta.term) {
              _insync_term = last_term;
          }
      });
}

void seq_stm::compact_snapshot() {
    auto cutoff_timestamp
      = model::timestamp::now().value()
        - _config.transactional_id_expiration_ms.value().count();
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
