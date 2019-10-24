#include "cluster/simple_batch_builder.h"

namespace cluster {

simple_batch_builder::simple_batch_builder(model::record_batch_type bt)
  : _batch_type(bt) {
}

model::record_batch simple_batch_builder::build() && {
    int32_t offset_delta = 0;
    uint32_t batch_size = storage::packed_header_size;
    crc32 crc;
    using ms = std::chrono::milliseconds;
    auto now_ts = std::chrono::duration_cast<ms>(
                    model::timeout_clock::now().time_since_epoch())
                    .count();
    std::vector<model::record> records;
    records.reserve(_records.size());

    model::record_batch_header header = {
      .size_bytes = 0,
      .base_offset = model::offset{},
      .type = _batch_type,
      .attrs = model::compute_batch_attributes(model::compression::none),
      .last_offset_delta = static_cast<int32_t>(_records.size() - 1),
      .first_timestamp = model::timestamp(now_ts),
      .max_timestamp = model::timestamp(now_ts)};

    storage::crc_batch_header(crc, header, _records.size());
    for (auto& sr : _records) {
        // create the record
        auto r = model::record(
          record_size(offset_delta, sr),
          model::record_attributes{},
          0,
          offset_delta++,
          std::move(sr.key),
          std::move(sr.value));

        storage::crc_record_header_and_key(
          crc,
          r.size_bytes(),
          r.attributes(),
          r.timestamp_delta(),
          r.offset_delta(),
          r.key());
        crc.extend(r.packed_value_and_headers());
        batch_size += r.size_bytes();
        batch_size += vint::vint_size(r.size_bytes());
        records.push_back(std::move(r));
    }
    header.size_bytes = batch_size;
    header.crc = crc.value();
    return model::record_batch(std::move(header), std::move(records));
}

uint32_t simple_batch_builder::record_size(
  int32_t offset_delta, const serialized_record& r) {
    return sizeof(int8_t) + vint::vint_size(offset_delta) + zero_vint_size
           + r.size_bytes();
}

} // namespace cluster