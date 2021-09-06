// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/segment_appender_utils.h"

#include "model/record.h"
#include "model/timestamp.h"
#include "reflection/adl.h"
#include "storage/logger.h"
#include "utils/vint.h"
#include "vassert.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/future-util.hh>

#include <bits/stdint-uintn.h>

#include <memory>
#include <type_traits>
namespace storage {

iobuf disk_header_to_iobuf(const model::record_batch_header& h) {
#ifndef NDEBUG
    vassert(h.header_crc != 0, "Header cannot have an unset crc:{}", h);
#endif
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

ss::future<>
write(segment_appender& appender, const model::record_batch& batch) {
    auto hdrbuf = std::make_unique<iobuf>(disk_header_to_iobuf(batch.header()));
    auto ptr = hdrbuf.get();
    return appender.append(*ptr).then(
      [&appender, &batch, cpy = std::move(hdrbuf)] {
          return appender.append(batch.data());
      });
}

} // namespace storage
