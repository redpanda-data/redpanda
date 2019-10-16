#pragma once

#include "seastarx.h"

#include <seastar/util/log.hh>

namespace storage {

inline logger& stlog() {
    static logger _stlog{"storage"};
    return _stlog;
}

} // namespace storage
