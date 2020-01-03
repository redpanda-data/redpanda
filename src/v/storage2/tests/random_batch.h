#pragma once 

#include "model/limits.h"
#include "model/compression.h"
#include "storage/tests/random_batch.h"

namespace storage::test {

static model::record_batch make_random_batch_v2(
  size_t num_records, bool allow_compression, size_t ts_order) {
    crc32 crc;

    // in real world system, timestamps are always increasing, so here the
    // ts_order variable is used to assign windows of randomness for timestamp
    // in such a way that they do not overlap, and that for each ts_order[n],
    // the timestamp window will always be smaller than for ts_order[n + 1]

    const size_t ts_window_size = num_records * 100;
    const size_t ts_window_begin = ts_order * ts_window_size;
    const size_t ts_window_end = (ts_order + 1) * ts_window_size - 1;

    auto timestamp = model::timestamp(
      random_generators::get_int<model::timestamp::value_type>(
        ts_window_size, ts_window_end + 2));

    // the model offset will be assigned by the partition, so here
    // whatever we set as the offset will be overwritten anyway, that's
    // why here the model::offset parameter is removed from the signature.
    auto offset = model::model_limits<model::offset>::min();
    auto header = make_random_header(
      offset, timestamp, num_records, allow_compression);
    storage::crc_batch_header(crc, header, num_records);
    auto size = packed_header_size;
    model::record_batch::records_type records;
    if (header.attrs.compression() != model::compression::none) {
        auto blob = make_random_ftb(random_generators::get_int(1024, 4096));
        size += blob.size_bytes();
        auto cr = model::record_batch::compressed_records(
          num_records, std::move(blob));
        crc.extend(cr.records());
        records = std::move(cr);
    } else {
        auto rs = model::record_batch::uncompressed_records();
        for (auto i = 0; i < num_records; ++i) {
            auto r = make_random_record(i);
            size += r.size_bytes();
            size += internal::vint_size(r.size_bytes());
            storage::crc_record_header_and_key(crc, r);
            crc.extend(r.packed_value_and_headers());
            rs.push_back(std::move(r));
        }
        records = std::move(rs);
    }
    header.size_bytes = size;
    header.crc = crc.value();
    return model::record_batch(std::move(header), std::move(records));
}

static model::record_batch
make_random_batch_v2(size_t num_records, bool allow_compression) {
    return make_random_batch_v2(num_records, allow_compression, 0);
}

static model::record_batch make_random_batch_v2(size_t num_records) {
    return make_random_batch_v2(num_records, false, 0);
}

} // namespace storage::test