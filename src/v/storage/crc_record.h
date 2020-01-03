#pragma once

#include "hashing/crc32c.h"
#include "model/record.h"

namespace storage {

void crc_batch_header(
  crc32&, const model::record_batch_header&, size_t num_records);

void crc_record_header_and_key(
  crc32&,
  size_t size_bytes,
  const model::record_attributes& attributes,
  int32_t timestamp_delta,
  int32_t offset_delta,
  const iobuf& key);

void crc_record_header_and_key(crc32&, const model::record& r);

} // namespace storage
