// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/segment_appender_utils.h"

#include "model/record.h"
#include "reflection/adl.h"

namespace storage {

iobuf batch_header_to_disk_iobuf(const model::record_batch_header& h) {
    iobuf b;
    reflection::serialize(
      b,
      h.header_crc,
      h.size_bytes,
      h.base_offset(),
      h.type,
      h.crc,
      h.attrs.value(),
      h.last_offset_delta,
      h.first_timestamp.value(),
      h.max_timestamp.value(),
      h.producer_id,
      h.producer_epoch,
      h.base_sequence,
      h.record_count);
    vassert(
      b.size_bytes() == model::packed_record_batch_header_size,
      "disk headers must be of static size:{}, but got{}",
      model::packed_record_batch_header_size,
      b.size_bytes());
    return b;
}

} // namespace storage
