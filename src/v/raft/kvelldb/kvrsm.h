#pragma once

#include "model/record.h"
#include "raft/consensus.h"
#include "raft/errc.h"
#include "raft/kvelldb/errc.h"
#include "raft/state_machine.h"
#include "utils/expiring_promise.h"
#include "utils/mutex.h"

#include <absl/container/flat_hash_map.h>

namespace raft::kvelldb {

class kvrsm : public state_machine {
public:
    struct cmd_result {
        cmd_result(ss::sstring wid, ss::sstring val)
          : write_id(wid)
          , value(val) {}
        cmd_result(
          ss::sstring wid,
          ss::sstring val,
          std::error_code rsm_err,
          std::error_code raft_err)
          : write_id(wid)
          , value(val)
          , rsm_status(rsm_err)
          , raft_status(raft_err) {}

        ss::sstring write_id;
        ss::sstring value;
        std::error_code rsm_status{raft::kvelldb::errc::success};
        std::error_code raft_status{raft::errc::success};
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
        ss::sstring key;
        ss::sstring value;
        ss::sstring write_id;
    };

    struct cas_cmd {
        static constexpr uint8_t record_key = 1;
        ss::sstring key;
        ss::sstring prev_write_id;
        ss::sstring value;
        ss::sstring write_id;
    };

    struct get_cmd {
        static constexpr uint8_t record_key = 2;
        ss::sstring key;
    };

    cmd_result process(model::record_batch&& b);
    cmd_result execute(set_cmd c);
    cmd_result execute(get_cmd c);
    cmd_result execute(cas_cmd c);

    ss::future<> apply(model::record_batch b) override;
    ss::future<result<raft::replicate_result>> replicate(model::record_batch&&);
    ss::future<cmd_result> replicate_and_wait(
      model::record_batch&& b, model::timeout_clock::time_point timeout);

    consensus* _c;
    absl::flat_hash_map<model::offset, expiring_promise<cmd_result>> _promises;
    absl::flat_hash_map<ss::sstring, kvrsm::record> kv_map;

    static inline constexpr model::record_batch_type kvrsm_batch_type
      = model::record_batch_type(10);
};

} // namespace raft::kvelldb