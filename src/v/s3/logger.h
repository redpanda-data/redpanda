#pragma once

#include "seastarx.h"

#include <seastar/util/log.hh>

namespace s3 {
inline ss::logger s3_log("s3");
} // namespace s3
