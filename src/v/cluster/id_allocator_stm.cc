// Copyright 2020 Redpanda Data, Inc.
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

#include <seastar/core/coroutine.hh>
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

id_allocator_stm::id_allocator_stm(ss::logger& logger, raft::consensus* c)
  : id_allocator_stm(logger, c, config::shard_local_cfg()) {}

id_allocator_stm::id_allocator_stm(
  ss::logger& logger, raft::consensus* c, config::configuration& cfg)
  : persisted_stm("id.snapshot", logger, c)
  , _batch_size(cfg.id_allocator_batch_size.value())
  , _log_capacity(cfg.id_allocator_log_capacity.value()) {}

ss::future<bool>
id_allocator_stm::sync(model::timeout_clock::duration timeout) {
    auto term = _insync_term;
    auto is_synced = co_await persisted_stm::sync(timeout);
    if (is_synced) {
        if (term != _insync_term) {
            _curr_id = _state;
            _curr_batch = 0;
            _processed = 0;
            _next_snapshot = _insync_offset;
        }
        if (_procesing_legacy) {
            for (auto& cmd : _cache) {
                _state += cmd.range;
            }
            is_synced = co_await set_state(_state + 1, timeout);
            if (is_synced) {
                _cache.clear();
                _procesing_legacy = false;
            }
        }
    }
    co_return is_synced;
}

ss::future<bool> id_allocator_stm::set_state(
  int64_t value, model::timeout_clock::duration timeout) {
    auto batch = serialize_cmd(
      state_cmd{.next_state = value}, model::record_batch_type::id_allocator);
    auto reader = model::make_memory_record_batch_reader(std::move(batch));
    auto r = co_await _c->replicate(
      _insync_term,
      std::move(reader),
      raft::replicate_options(raft::consistency_level::quorum_ack));
    if (!r) {
        co_return false;
    }
    if (!co_await wait_no_throw(
          model::offset(r.value().last_offset()),
          model::timeout_clock::now() + timeout)) {
        co_return false;
    }
    co_return true;
}

ss::future<id_allocator_stm::stm_allocation_result>
id_allocator_stm::allocate_id(model::timeout_clock::duration timeout) {
    return _lock
      .with(timeout, [this, timeout]() { return do_allocate_id(timeout); })
      .handle_exception_type([](const ss::semaphore_timed_out&) {
          return stm_allocation_result{-1, raft::errc::timeout};
      });
}

ss::future<id_allocator_stm::stm_allocation_result>
id_allocator_stm::do_allocate_id(model::timeout_clock::duration timeout) {
    if (!co_await sync(timeout)) {
        co_return stm_allocation_result{-1, raft::errc::timeout};
    }

    if (_curr_batch == 0) {
        _curr_id = _state;
        if (!co_await set_state(_curr_id + _batch_size, timeout)) {
            co_return stm_allocation_result{-1, raft::errc::timeout};
        }
        _curr_batch = _batch_size;
    }

    auto id = _curr_id;

    _curr_id += 1;
    _curr_batch -= 1;

    co_return stm_allocation_result{id, raft::errc::success};
}

ss::future<> id_allocator_stm::apply(model::record_batch b) {
    if (b.header().type != model::record_batch_type::id_allocator) {
        return ss::now();
    }

    vassert(b.record_count() == 1, "We expect single command in single batch");
    auto r = b.copy_records();
    auto& record = *r.begin();
    auto rk = reflection::adl<uint8_t>{}.from(record.release_key());

    _insync_offset = b.last_offset();

    if (rk == allocation_cmd::record_key) {
        allocation_cmd cmd = reflection::adl<allocation_cmd>{}.from(
          record.release_value());
        if (_should_cache) {
            _cache.emplace_back(cmd);
        } else {
            _state += cmd.range;
        }
    } else if (rk == prepare_truncation_cmd::record_key) {
        if (!_should_cache) {
            _should_cache = true;
            _prepare_offset = b.last_offset();
        }
    } else if (rk == execute_truncation_cmd::record_key) {
        execute_truncation_cmd cmd = reflection::adl<execute_truncation_cmd>{}
                                       .from(record.release_value());
        if (_should_cache && _prepare_offset == cmd.prepare_offset) {
            _state = cmd.state;
            for (auto& cmd : _cache) {
                _state += cmd.range;
            }
            _cache.clear();
            _should_cache = false;
        }
    } else if (rk == state_cmd::record_key) {
        _procesing_legacy = false;
        state_cmd cmd = reflection::adl<state_cmd>{}.from(
          record.release_value());
        _state = cmd.next_state;

        if (_next_snapshot() < 0) {
            _next_snapshot = _insync_offset;
            _processed = 0;
        }

        _processed++;
        if (_processed > _log_capacity) {
            ssx::spawn_with_gate(_gate, [this] { return write_snapshot(); });
        }
    }

    return ss::now();
}

ss::future<> id_allocator_stm::write_snapshot() {
    if (_is_writing_snapshot) {
        return ss::now();
    }
    if (_processed <= _log_capacity) {
        return ss::now();
    }
    _is_writing_snapshot = true;
    return _c->write_snapshot(raft::write_snapshot_cfg(_next_snapshot, iobuf()))
      .then([this] {
          _next_snapshot = _insync_offset;
          _processed = 0;
      })
      .finally([this] { _is_writing_snapshot = false; });
}

ss::future<> id_allocator_stm::apply_snapshot(stm_snapshot_header, iobuf&&) {
    return ss::make_exception_future<>(
      std::logic_error("id_allocator_stm doesn't support snapshots"));
}

ss::future<stm_snapshot> id_allocator_stm::take_snapshot() {
    return ss::make_exception_future<stm_snapshot>(
      std::logic_error("id_allocator_stm doesn't support snapshots"));
}

ss::future<> id_allocator_stm::handle_eviction() {
    _next_snapshot = _c->start_offset();
    _processed = 0;
    set_next(_next_snapshot);
    _insync_offset = model::prev_offset(_next_snapshot);
    return ss::now();
}

} // namespace cluster
