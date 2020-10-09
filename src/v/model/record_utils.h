#pragma once

#include "bytes/iobuf_parser.h"
#include "hashing/crc32c.h"

namespace model {

struct record_batch_header;
class record_batch;
class record;

void crc_record_batch_header(crc32&, const record_batch_header&);

void crc_record_batch(crc32&, const record_batch&);

/// \brief int32_t because that's what kafka uses
int32_t crc_record_batch(const record_batch& b);
int32_t crc_record_batch(const record_batch_header&, const iobuf&);

int32_t recompute_record_batch_size(const record_batch& b);

/// \brief uint32_t because that's what crc32c uses
/// it is *only* record_batch_header.header_crc;
uint32_t internal_header_only_crc(const record_batch_header&);

model::record parse_one_record_from_buffer(iobuf_parser& parser);
model::record parse_one_record_copy_from_buffer(iobuf_const_parser& parser);
void append_record_to_buffer(iobuf& a, const model::record& r);

} // namespace model
