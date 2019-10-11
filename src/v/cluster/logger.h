#pragma once
#include "seastarx.h"

#include <seastar/core/print.hh>
#include <seastar/util/log.hh>

namespace cluster {
inline logger& clusterlog() {
    static logger _cluster_log{"cluster"};
    return _cluster_log;
}
} // namespace cluster
