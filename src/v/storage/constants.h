#pragma once

#include "model/record.h"

namespace storage {

constexpr unsigned packed_header_size
  = sizeof(model::record_batch_header::base_offset)
    + sizeof(model::record_batch_header::crc)
    + sizeof(model::record_batch_attributes::value_type)
    + sizeof(model::record_batch_header::last_offset_delta)
    + sizeof(model::record_batch_header::first_timestamp)
    + sizeof(model::record_batch_header::max_timestamp)
    + sizeof(int32_t); // num_records
} // namespace storage