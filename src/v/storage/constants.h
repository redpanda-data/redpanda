#pragma once

#include "model/record.h"

namespace storage {

// Total: 39 bytes
constexpr uint32_t packed_header_size
  = sizeof(model::record_batch_header::base_offset)         // 8
    + sizeof(model::record_batch_type::type)                // 1
    + sizeof(model::record_batch_header::crc)               // 4
    + sizeof(model::record_batch_attributes::type)          // 2
    + sizeof(model::record_batch_header::last_offset_delta) // 4
    + sizeof(model::record_batch_header::first_timestamp)   // 8
    + sizeof(model::record_batch_header::max_timestamp)     // 8
    + /*num of records*/ sizeof(int32_t);                   // 4
} // namespace storage
