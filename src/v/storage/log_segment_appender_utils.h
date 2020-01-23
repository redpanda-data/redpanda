#pragma once
#include "model/record.h"
#include "storage/log_segment_appender.h"
namespace storage {

ss::future<> write(log_segment_appender& out, const model::record& record);

ss::future<>
write(log_segment_appender& appender, const model::record_batch& batch);

} // namespace storage
