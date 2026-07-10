// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/record_batch_utils.h"

#include "model/record.h"
#include "reflection/adl.h"

namespace storage {

iobuf batch_header_to_disk_iobuf(const model::record_batch_header& h) {
    iobuf b;
    b.append(model::pack_record_batch_header(h));
    vassert(
      b.size_bytes() == model::packed_record_batch_header_size,
      "disk headers must be of static size:{}, but got{}",
      model::packed_record_batch_header_size,
      b.size_bytes());
    return b;
}

model::record_batch_header batch_header_from_disk_iobuf(iobuf b) {
    iobuf_parser parser(std::move(b));
    model::packed_record_batch_header encoded;
    parser.consume_to(encoded.size(), encoded.begin());
    vassert(
      parser.bytes_consumed() == model::packed_record_batch_header_size,
      "Error in header parsing. Must consume:{} bytes, but consumed:{}",
      model::packed_record_batch_header_size,
      parser.bytes_consumed());
    auto hdr = model::unpack_record_batch_header(encoded);
    hdr.ctx.owner_shard = ss::this_shard_id();
    return hdr;
}

} // namespace storage
