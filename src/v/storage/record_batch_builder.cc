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
      .producer_id = -1,
      .producer_epoch = -1,
      .base_sequence = -1,
      .record_count = static_cast<int32_t>(_records.size()),
      .ctx = model::record_batch_header::context(
        model::term_id(0), ss::this_shard_id())};

    iobuf records;
    for (auto& sr : _records) {
        auto rec_sz = record_size(offset_delta, sr);
        auto kz = sr.key.size_bytes();
        auto vz = sr.value.size_bytes();
        auto r = model::record(
          rec_sz,
          model::record_attributes{},
          0,
          offset_delta,
          kz,
          std::move(sr.key),
          vz,
          std::move(sr.value),
          std::vector<model::record_header>{});
        ++offset_delta;
        model::append_record_to_buffer(records, r);
    }

    internal::reset_size_checksum_metadata(header, records);
    return model::record_batch(
      header, std::move(records), model::record_batch::tag_ctor_ng{});
}

uint32_t record_batch_builder::record_size(
  int32_t offset_delta, const serialized_record& r) {
    return sizeof(model::record_attributes::type)  // attributes
           + zero_vint_size                        // timestamp delta
           + vint::vint_size(offset_delta)         // offset_delta
           + vint::vint_size(r.key.size_bytes())   // key size
           + r.key.size_bytes()                    // key
           + vint::vint_size(r.value.size_bytes()) // value size
           + r.value.size_bytes()                  // value
           + zero_vint_size;                       // headers size
}

} // namespace storage
