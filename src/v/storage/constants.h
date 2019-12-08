#pragma once

#include "model/record.h"

namespace storage {

constexpr uint32_t packed_header_size
  = sizeof(model::record_batch_header::base_offset)
    + sizeof(model::record_batch_type::type)
    + sizeof(model::record_batch_header::crc)
    + sizeof(model::record_batch_attributes::value_type)
    + sizeof(model::record_batch_header::last_offset_delta)
    + sizeof(model::record_batch_header::first_timestamp)
    + sizeof(model::record_batch_header::max_timestamp)
    + sizeof(int32_t); // num_records
} // namespace storage
