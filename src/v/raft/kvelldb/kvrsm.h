/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "model/fundamental.h"
#include "model/record.h"
#include "raft/errc.h"
#include "raft/kvelldb/errc.h"
#include "raft/state_machine.h"
#include "utils/expiring_promise.h"
#include "utils/mutex.h"

#include <absl/container/flat_hash_map.h>

namespace raft::kvelldb {

class kvrsm : public state_machine {
public:
    using sequence_id = named_type<uint64_t, struct kvrsm_sequence_id>;
    using us = std::chrono::microseconds;

    struct cmd_result {
        cmd_result(sequence_id seq, ss::sstring wid, ss::sstring val)
          : seq(seq)
          , write_id(wid)
          , value(val) {}

        cmd_result(std::error_code rsm_err, std::error_code raft_err)
          : seq(sequence_id(0))
          , write_id("")
          , value("")
          , rsm_status(rsm_err)
          , raft_status(raft_err) {}

        cmd_result(
          sequence_id seq, std::error_code rsm_err, std::error_code raft_err)
          : seq(seq)
          , write_id("")
          , value("")
          , rsm_status(rsm_err)
          , raft_status(raft_err) {}

        cmd_result(
          sequence_id seq,
          ss::sstring wid,
          ss::sstring val,
          std::error_code rsm_err,
          std::error_code raft_err)
          : seq(seq)
          , write_id(wid)
          , value(val)
          , rsm_status(rsm_err)
          , raft_status(raft_err) {}

        sequence_id seq;
        ss::sstring write_id;
        ss::sstring value;
        std::error_code rsm_status{raft::kvelldb::errc::success};
        std::error_code raft_status{raft::errc::success};
        model::offset offset;
        us replicated_us;
        us executed_us;
    };

    explicit kvrsm(ss::logger&, consensus*);

    ss::future<cmd_result> set_and_wait(
      ss::sstring key,
      ss::sstring value,
      ss::sstring write_id,
      model::timeout_clock::time_point timeout);

    ss::future<cmd_result> cas_and_wait(
      ss::sstring key,
      ss::sstring prev_write_id,
      ss::sstring value,
      ss::sstring write_id,
      model::timeout_clock::time_point timeout);

    ss::future<cmd_result>
    get_and_wait(ss::sstring key, model::timeout_clock::time_point timeout);

private:
    struct record {
        ss::sstring value;
        ss::sstring write_id;
    };

    struct set_cmd {
        static constexpr uint8_t record_key = 0;
        sequence_id seq;
        ss::sstring key;
        ss::sstring value;
        ss::sstring write_id;
    };

    struct cas_cmd {
        static constexpr uint8_t record_key = 1;
        sequence_id seq;
        ss::sstring key;
        ss::sstring prev_write_id;
        ss::sstring value;
        ss::sstring write_id;
    };

    struct get_cmd {
        static constexpr uint8_t record_key = 2;
        sequence_id seq;
        ss::sstring key;
    };

    cmd_result process(model::record_batch&& b);
    cmd_result execute(set_cmd c);
    cmd_result execute(get_cmd c);
    cmd_result execute(cas_cmd c);

    ss::future<> apply(model::record_batch b) override;
    ss::future<result<raft::replicate_result>> replicate(model::record_batch&&);
    ss::future<cmd_result> replicate_and_wait(
      model::record_batch&& b,
      model::timeout_clock::time_point timeout,
      sequence_id seq);

    sequence_id _last_applied_seq;
    sequence_id _last_generated_seq;
    consensus* _c;
    absl::
      flat_hash_map<sequence_id, std::unique_ptr<expiring_promise<cmd_result>>>
        _promises;
    absl::flat_hash_map<ss::sstring, kvrsm::record> kv_map;
};

} // namespace raft::kvelldb
