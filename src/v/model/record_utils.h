/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "bytes/iobuf_parser.h"
#include "hashing/crc32c.h"

namespace model {

struct record_batch_header;
class record_batch;
class record;

void crc_record_batch_header(crc::crc32c&, const record_batch_header&);

uint32_t crc_record_batch(const record_batch& b);
uint32_t crc_record_batch(const record_batch_header&, const iobuf&);

/// \brief uint32_t because that's what crc32c uses
/// it is *only* record_batch_header.header_crc;
uint32_t internal_header_only_crc(const record_batch_header&);

model::record parse_one_record_from_buffer(iobuf_parser& parser);
model::record parse_one_record_copy_from_buffer(iobuf_const_parser& parser);
void append_record_to_buffer(iobuf& a, const model::record& r);

} // namespace model
