// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/kvelldb/kvrsm.h"

#include "raft/consensus.h"
#include "raft/errc.h"
#include "raft/kvelldb/errc.h"
#include "raft/types.h"
#include "storage/record_batch_builder.h"

namespace raft::kvelldb {

template<typename T>
model::record_batch serialize_cmd(T t, model::record_batch_type type) {
    storage::record_batch_builder b(type, model::offset(0));
    iobuf key_buf;
    reflection::adl<uint8_t>{}.to(key_buf, T::record_key);
    iobuf v_buf;
    reflection::adl<T>{}.to(v_buf, std::forward<T>(t));
    b.add_raw_kv(std::move(key_buf), std::move(v_buf));
    return std::move(b).build();
}

kvrsm::kvrsm(ss::logger& logger, consensus* c)
  : raft::state_machine(c, logger, ss::default_priority_class())
  , _c(c) {
    _last_applied_seq = sequence_id(0);
    _last_generated_seq = sequence_id(0);
}

ss::future<kvrsm::cmd_result> kvrsm::set_and_wait(
  ss::sstring key,
  ss::sstring value,
  ss::sstring write_id,
  model::timeout_clock::time_point timeout) {
    sequence_id seq = ++_last_generated_seq;
    return replicate_and_wait(
      serialize_cmd(
        set_cmd{seq, key, value, write_id}, model::record_batch_type::kvstore),
      timeout,
      seq);
}

ss::future<kvrsm::cmd_result> kvrsm::cas_and_wait(
  ss::sstring key,
  ss::sstring prev_write_id,
  ss::sstring value,
  ss::sstring write_id,
  model::timeout_clock::time_point timeout) {
    sequence_id seq = ++_last_generated_seq;
    return replicate_and_wait(
      serialize_cmd(
        cas_cmd{seq, key, prev_write_id, value, write_id},
        model::record_batch_type::kvstore),
      timeout,
      seq);
}

ss::future<kvrsm::cmd_result>
kvrsm::get_and_wait(ss::sstring key, model::timeout_clock::time_point timeout) {
    sequence_id seq = ++_last_generated_seq;
    return replicate_and_wait(
      serialize_cmd(get_cmd{seq, key}, model::record_batch_type::kvstore),
      timeout,
      seq);
}

ss::future<> kvrsm::apply(model::record_batch b) {
    if (b.header().type == model::record_batch_type::kvstore) {
        auto last_offset = b.last_offset();
        auto result = process(std::move(b));
        result.offset = last_offset;

        if (auto it = _promises.find(result.seq); it != _promises.end()) {
            it->second->set_value(result);
        }
    }

    return ss::now();
}

kvrsm::cmd_result kvrsm::process(model::record_batch&& b) {
    auto r = b.copy_records();
    auto rk = reflection::adl<uint8_t>{}.from(r.begin()->release_key());

    if (rk == set_cmd::record_key) {
        return execute(
          reflection::adl<set_cmd>{}.from(r.begin()->release_value()));
    } else if (rk == get_cmd::record_key) {
        return execute(
          reflection::adl<get_cmd>{}.from(r.begin()->release_value()));
    } else if (rk == cas_cmd::record_key) {
        return execute(
          reflection::adl<cas_cmd>{}.from(r.begin()->release_value()));
    } else {
        return kvrsm::cmd_result(
          raft::kvelldb::errc::unknown_command, raft::errc::success);
    }
}

kvrsm::cmd_result kvrsm::execute(set_cmd c) {
    if (c.seq <= _last_applied_seq) {
        return kvrsm::cmd_result(
          c.seq,
          raft::kvelldb::errc::raft_error,
          raft::errc::leader_append_failed);
    }
    _last_applied_seq = c.seq;
    if (c.seq > _last_generated_seq) {
        _last_generated_seq = c.seq;
    }

    if (kv_map.contains(c.key)) {
        auto& record = kv_map[c.key];
        record.write_id = c.write_id;
        record.value = c.value;
        return kvrsm::cmd_result(c.seq, record.write_id, record.value);
    } else {
        kvrsm::record record;
        record.write_id = c.write_id;
        record.value = c.value;
        kv_map.emplace(c.key, record);
        return kvrsm::cmd_result(c.seq, record.write_id, record.value);
    }
}

kvrsm::cmd_result kvrsm::execute(get_cmd c) {
    if (c.seq <= _last_applied_seq) {
        return kvrsm::cmd_result(
          c.seq,
          raft::kvelldb::errc::raft_error,
          raft::errc::leader_append_failed);
    }
    _last_applied_seq = c.seq;
    if (c.seq > _last_generated_seq) {
        _last_generated_seq = c.seq;
    }

    if (kv_map.contains(c.key)) {
        return kvrsm::cmd_result(
          c.seq,
          kv_map.find(c.key)->second.write_id,
          kv_map.find(c.key)->second.value);
    }
    return kvrsm::cmd_result(
      c.seq, raft::kvelldb::errc::not_found, raft::errc::success);
}

kvrsm::cmd_result kvrsm::execute(cas_cmd c) {
    if (c.seq <= _last_applied_seq) {
        return kvrsm::cmd_result(
          c.seq,
          raft::kvelldb::errc::raft_error,
          raft::errc::leader_append_failed);
    }
    _last_applied_seq = c.seq;
    if (c.seq > _last_generated_seq) {
        _last_generated_seq = c.seq;
    }

    if (kv_map.contains(c.key)) {
        auto& record = kv_map[c.key];

        if (record.write_id == c.prev_write_id) {
            record.write_id = c.write_id;
            record.value = c.value;
            return kvrsm::cmd_result(c.seq, record.write_id, record.value);
        } else {
            return kvrsm::cmd_result(
              c.seq,
              record.write_id,
              record.value,
              raft::kvelldb::errc::conflict,
              raft::errc::success);
        }
    } else {
        return kvrsm::cmd_result(
          c.seq, raft::kvelldb::errc::not_found, raft::errc::success);
    }
}

ss::future<kvrsm::cmd_result> kvrsm::replicate_and_wait(
  model::record_batch&& b,
  model::timeout_clock::time_point timeout,
  sequence_id seq) {
    using ret_t = kvrsm::cmd_result;

    auto started = std::chrono::steady_clock::now();

    _promises.emplace(seq, std::make_unique<expiring_promise<ret_t>>());

    return replicate(std::move(b))
      .then([this, seq, timeout, started](result<raft::replicate_result> r) {
          auto replicated_us
            = std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::steady_clock::now() - started);

          if (!r) {
              _promises.erase(seq);
              auto result = kvrsm::cmd_result(
                raft::kvelldb::errc::raft_error, r.error());
              result.replicated_us = replicated_us;
              return ss::make_ready_future<ret_t>(result);
          }

          auto last_offset = r.value().last_offset;

          auto it = _promises.find(seq);

          return it->second
            ->get_future_with_timeout(
              timeout,
              [seq, last_offset] {
                  auto result = kvrsm::cmd_result(
                    raft::kvelldb::errc::timeout, raft::errc::timeout);
                  result.seq = seq;
                  result.offset = last_offset;
                  return result;
              })
            .then_wrapped([this, seq, last_offset, started, replicated_us](
                            ss::future<ret_t> ec) {
                auto executed_us
                  = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() - started);
                _promises.erase(seq);

                return ec.then([last_offset, replicated_us, executed_us](
                                 kvrsm::cmd_result result) {
                    if (result.offset != last_offset) {
                        result = kvrsm::cmd_result(
                          raft::kvelldb::errc::raft_error,
                          raft::errc::leader_append_failed);
                    }

                    result.replicated_us = replicated_us;
                    result.executed_us = executed_us;
                    return ss::make_ready_future<ret_t>(result);
                });
            });
      });
}

ss::future<result<raft::replicate_result>>
kvrsm::replicate(model::record_batch&& batch) {
    return _c->replicate(
      model::make_memory_record_batch_reader(std::move(batch)),
      raft::replicate_options{raft::consistency_level::quorum_ack});
}

} // namespace raft::kvelldb
