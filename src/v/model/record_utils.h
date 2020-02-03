#pragma once

#include "hashing/crc32c.h"
#include "model/record.h"

namespace model {

void crc_record_batch_header(crc32&, const record_batch_header&);

void crc_record(crc32&, const record& r);

void crc_record_batch(crc32&, const record_batch&);

int32_t crc_record_batch(const record_batch& b);

} // namespace model
