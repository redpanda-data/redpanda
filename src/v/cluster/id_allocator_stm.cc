// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/id_allocator_stm.h"

#include "cluster/logger.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "raft/consensus.h"
#include "raft/errc.h"
#include "raft/types.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/future.hh>

namespace cluster {

template<typename T>
static model::record_batch serialize_cmd(T t, model::record_batch_type type) {
    storage::record_batch_builder b(type, model::offset(0));
    iobuf key_buf;
    reflection::adl<uint8_t>{}.to(key_buf, T::record_key);
    iobuf v_buf;
    reflection::adl<T>{}.to(v_buf, std::move(t));
    b.add_raw_kv(std::move(key_buf), std::move(v_buf));
    return std::move(b).build();
}

id_allocator_stm::id_allocator_stm(
  ss::logger& logger, raft::consensus* c, config::configuration& config)
  : raft::state_machine(c, logger, ss::default_priority_class())
  , _config(config)
  , _last_seq_tick(0)
  , _c(c) {}

ss::future<id_allocator_stm::stm_allocation_result>
id_allocator_stm::allocate_id_and_wait(
  model::timeout_clock::time_point timeout) {
    auto prelude = ss::now();
    auto range = _config.id_allocator_batch_size.value();

    if (_last_allocated_range > 0) {
        auto allocated_id = _last_allocated_base;
        _last_allocated_range -= 1;
        _last_allocated_base += 1;

        return ss::make_ready_future<stm_allocation_result>(
          stm_allocation_result{allocated_id, raft::errc::success});
    }

    if (_processed > _config.id_allocator_log_capacity.value()) {
        auto seq = sequence_id{_run_id.value(), _c->self(), ++_last_seq_tick};
        prelude = replicate_and_wait(prepare_truncation_cmd{seq}, timeout, seq)
                    .then([this](bool replicated) {
                        if (replicated) {
                            try {
                                (void)ss::with_gate(_gate, [this] {
                                    return replicate(serialize_cmd(
                                      execute_truncation_cmd{
                                        _prepare_offset, _prepare_state},
                                      cluster::id_allocator_stm_batch_type));
                                });
                            } catch (const ss::gate_closed_exception&) {
                                // it's ok to ignore the expection because
                                // it doesn't lead to any violation and
                                // we can't do anything
                            }
                        }
                    });
    } else if (_should_cache) {
        try {
            (void)ss::with_gate(_gate, [this] {
                return replicate(serialize_cmd(
                  execute_truncation_cmd{_prepare_offset, _prepare_state},
                  cluster::id_allocator_stm_batch_type));
            });
        } catch (const ss::gate_closed_exception&) {
            // it's ok to ignore the expection because
            // it doesn't lead to any violation and
            // we can't do anything
        }
    }

    return prelude.then([this, timeout, range] {
        sequence_id seq = sequence_id{
          _run_id.value(), _c->self(), ++_last_seq_tick};

        return replicate_and_wait(allocation_cmd{seq, range}, timeout, seq)
          .then([this](log_allocation_result r) {
              _last_allocated_base = r.base + 1;
              _last_allocated_range = r.range - 1;
              return stm_allocation_result{r.base, r.raft_status};
          });
    });
}

ss::future<> id_allocator_stm::apply(model::record_batch b) {
    if (b.header().type == cluster::id_allocator_stm_batch_type) {
        return process(std::move(b));
    }

    return ss::now();
}

ss::future<> id_allocator_stm::start() {
    auto last = _c->read_last_applied();
    if (last != model::offset{}) {
        set_next(last);
    }

    return state_machine::start().then([this] {
        return _c->get_run_id().then(
          [this](model::run_id id) { _run_id.emplace(id); });
    });
}

ss::future<> id_allocator_stm::process(model::record_batch&& b) {
    vassert(b.record_count() == 1, "We expect single command in single batch");
    auto r = b.copy_records();
    auto& record = *r.begin();
    auto rk = reflection::adl<uint8_t>{}.from(record.release_key());

    if (rk == allocation_cmd::record_key) {
        allocation_cmd cmd = reflection::adl<allocation_cmd>{}.from(
          record.release_value());
        if (_should_cache) {
            _cache.emplace_back(cached_allocation_cmd{b.last_offset(), cmd});
        } else {
            execute(b.last_offset(), cmd);
        }
    } else if (rk == prepare_truncation_cmd::record_key) {
        prepare_truncation_cmd cmd = reflection::adl<prepare_truncation_cmd>{}
                                       .from(record.release_value());
        if (!_should_cache) {
            _processed = 0;
            _should_cache = true;
            _prepare_state = _state;
            _prepare_offset = b.last_offset();
        }
        if (auto it = _prepare_promises.find(cmd.seq);
            it != _prepare_promises.end()) {
            it->second.set_value(true);
        }
    } else if (rk == execute_truncation_cmd::record_key) {
        execute_truncation_cmd cmd = reflection::adl<execute_truncation_cmd>{}
                                       .from(record.release_value());
        if (_should_cache && _prepare_offset == cmd.prepare_offset) {
            _state = cmd.state;
            for (auto& it : _cache) {
                execute(it.offset, it.cmd);
            }
            _cache.clear();
            _should_cache = false;
            return _c->write_last_applied(_prepare_offset).then([this] {
                return _c->write_snapshot(raft::write_snapshot_cfg(
                  model::offset(_prepare_offset - 1),
                  iobuf(),
                  raft::write_snapshot_cfg::should_prefix_truncate::yes));
            });
        }
    }
    return ss::now();
}

void id_allocator_stm::execute(model::offset offset, allocation_cmd c) {
    auto base = _state;
    _state += c.range;
    _processed += 1;

    id_allocator_stm::log_allocation_result result{
      c.seq, base, c.range, raft::errc::success, offset};

    if (auto it = _promises.find(result.seq); it != _promises.end()) {
        it->second.set_value(result);
    }
}

ss::future<id_allocator_stm::log_allocation_result>
id_allocator_stm::replicate_and_wait(
  allocation_cmd cmd,
  model::timeout_clock::time_point timeout,
  sequence_id seq) {
    using ret_t = id_allocator_stm::log_allocation_result;

    auto b = serialize_cmd(cmd, cluster::id_allocator_stm_batch_type);
    _promises.emplace(seq, expiring_promise<ret_t>{});

    return replicate(std::move(b))
      .then([this, seq, timeout](result<raft::replicate_result> r) {
          if (!r) {
              _promises.erase(seq);
              auto result = id_allocator_stm::log_allocation_result{
                seq, 0, 0, raft::errc::timeout, model::offset(0)};
              return ss::make_ready_future<ret_t>(result);
          }

          auto last_offset = r.value().last_offset;

          auto it = _promises.find(seq);

          vassert(
            it != _promises.end(), "seq isn't in _promises after insertion");

          return it->second
            .get_future_with_timeout(
              timeout,
              [seq, last_offset] {
                  return id_allocator_stm::log_allocation_result{
                    seq, 0, 0, raft::errc::timeout, last_offset};
              })
            .then([this, seq, last_offset](ret_t r) {
                _promises.erase(seq);
                vassert(
                  r.offset == last_offset,
                  "seq uniqueness violation, got offset: {} expected offset: "
                  "{}",
                  r.offset,
                  last_offset);
                return ss::make_ready_future<ret_t>(r);
            });
      });
}

ss::future<bool> id_allocator_stm::replicate_and_wait(
  prepare_truncation_cmd cmd,
  model::timeout_clock::time_point timeout,
  sequence_id seq) {
    auto b = serialize_cmd(cmd, cluster::id_allocator_stm_batch_type);
    _prepare_promises.emplace(seq, expiring_promise<bool>{});

    return replicate(std::move(b))
      .then([this, seq, timeout](result<raft::replicate_result> r) {
          if (!r) {
              _prepare_promises.erase(seq);
              return ss::make_ready_future<bool>(false);
          }

          auto it = _prepare_promises.find(seq);

          vassert(
            it != _prepare_promises.end(),
            "seq isn't in _prepare_promises after insertion");

          return it->second
            .get_future_with_timeout(timeout, [] { return false; })
            .finally([this, seq] { _prepare_promises.erase(seq); });
      });
}

ss::future<result<raft::replicate_result>>
id_allocator_stm::replicate(model::record_batch&& batch) {
    return _c->replicate(
      model::make_memory_record_batch_reader(std::move(batch)),
      raft::replicate_options{raft::consistency_level::quorum_ack});
}

} // namespace cluster
