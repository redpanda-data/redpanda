#pragma once

#include "seastarx.h"

#include <seastar/util/log.hh>

namespace raft {
inline logger& raftlog() {
    static logger _g{"raft"};
    return _g;
}
} // namespace raft
