#pragma once
#include "bytes/iobuf.h"
#include "model/record.h"
#include "storage/segment_appender.h"

namespace storage {

ss::future<>
write(segment_appender& appender, const model::record_batch& batch);

} // namespace storage
