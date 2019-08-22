#pragma once
#include "seastarx.h"

#include <seastar/core/print.hh>
#include <seastar/util/log.hh>

namespace rpc {
inline logger& rpclog() {
    static logger _rpc_log{"rpc"};
    return _rpc_log;
}
} // namespace rpc
