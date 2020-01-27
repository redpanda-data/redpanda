#pragma once
#include "bytes/iobuf.h"
#include "model/record.h"
#include "storage/log_segment_appender.h"

namespace storage {

iobuf disk_header_to_iobuf(
  const model::record_batch_header&, uint32_t record_count);

ss::future<> write(log_segment_appender& out, const model::record& record);

ss::future<>
write(log_segment_appender& appender, const model::record_batch& batch);

} // namespace storage
