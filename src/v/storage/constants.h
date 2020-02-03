#pragma once

#include "model/record.h"

namespace storage {

// Total: 53 bytes
constexpr uint32_t packed_header_size
  = sizeof(model::record_batch_header::base_offset)         // 8
    + sizeof(model::record_batch_type::type)                // 1
    + sizeof(model::record_batch_header::crc)               // 4
    + sizeof(model::record_batch_attributes::type)          // 2
    + sizeof(model::record_batch_header::last_offset_delta) // 4
    + sizeof(model::record_batch_header::first_timestamp)   // 8
    + sizeof(model::record_batch_header::max_timestamp)     // 8
    + sizeof(model::record_batch_header::producer_id)       // 8
    + sizeof(model::record_batch_header::producer_epoch)    // 2
    + sizeof(model::record_batch_header::base_sequence)     // 4
    + sizeof(model::record_batch_header::record_count);     // 4
} // namespace storage
