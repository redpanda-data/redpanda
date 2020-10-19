#pragma once

#include "bytes/iobuf.h"
#include "model/async_adl_serde.h"
#include "raft/types.h"

namespace raft::tron {
struct stats_request {};
struct stats_reply {};
struct put_reply {
    bool success;
    ss::sstring failure_reason;
};
} // namespace raft::tron
