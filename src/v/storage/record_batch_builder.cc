// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/record_batch_builder.h"

#include "model/record.h"
#include "model/record_utils.h"
#include "model/timeout_clock.h"
#include "storage/parser_utils.h"

#include <seastar/core/smp.hh>

namespace storage {

record_batch_builder::record_batch_builder(
  model::record_batch_type bt, model::offset base_offset)
  : _batch_type(bt)
  , _base_offset(base_offset) {}

record_batch_builder::~record_batch_builder() {}

model::record_batch record_batch_builder::build() && {
    int32_t offset_delta = 0;
    auto now_ts = model::timestamp::now();

    model::record_batch_header header = {
      .size_bytes = 0,
      .base_offset = _base_offset,
      .type = _batch_type,
      .crc = 0, // crc computed later
      .attrs = model::record_batch_attributes{} |= model::compression::none,
      .last_offset_delta = static_cast<int32_t>(_records.size() - 1),
      .first_timestamp = now_ts,
      .max_timestamp = now_ts,
      .producer_id = _producer_id,
      .producer_epoch = _producer_epoch,
      .base_sequence = -1,
      .record_count = static_cast<int32_t>(_records.size()),
      .ctx = model::record_batch_header::context(
        model::term_id(0), ss::this_shard_id())};

    if (_is_control_type) {
        header.attrs.set_control_type();
    }

    if (_transactional_type) {
        header.attrs.set_transactional_type();
    }

    iobuf records;
    for (auto& sr : _records) {
        auto rec_sz = record_size(offset_delta, sr);
        auto kz = sr.key.size_bytes();
        auto vz = sr.encoded_value_size;
        auto r = model::record(
          rec_sz,
          model::record_attributes{},
          0,
          offset_delta,
          kz,
          std::move(sr.key),
          vz,
          std::move(sr.value),
          std::move(sr.headers));
        ++offset_delta;
        model::append_record_to_buffer(records, r);
    }

    internal::reset_size_checksum_metadata(header, records);
    return model::record_batch(
      header, std::move(records), model::record_batch::tag_ctor_ng{});
}

uint32_t record_batch_builder::record_size(
  int32_t offset_delta, const serialized_record& r) {
    uint32_t size = sizeof(model::record_attributes::type)  // attributes
                    + zero_vint_size                        // timestamp delta
                    + vint::vint_size(offset_delta)         // offset_delta
                    + vint::vint_size(r.key.size_bytes())   // key size
                    + r.key.size_bytes()                    // key
                    + vint::vint_size(r.encoded_value_size) // value size
                    + r.value.size_bytes()                  // value
                    + vint::vint_size(r.headers.size());    // headers size
    for (const auto& h : r.headers) {
        size += vint::vint_size(h.key_size()) + h.key().size_bytes()
                + vint::vint_size(h.value_size()) + h.value().size_bytes();
    }
    return size;
}

} // namespace storage
