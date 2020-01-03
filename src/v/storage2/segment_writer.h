#pragma once

#include "model/record.h"
#include "storage2/segment_appender.h"

#include <seastar/core/future.hh>

namespace storage {

future<> write_to(segment_appender&, model::record_batch&&);

} // namespace storage