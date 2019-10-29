#pragma once

#include "hashing/crc32c.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "random/generators.h"
#include "storage/constants.h"
#include "storage/crc_record.h"
#include "utils/fragbuf.h"
#include "utils/vint.h"

#include <random>

namespace storage::test {

namespace internal {
size_t vint_size(size_t val) {
    std::array<bytes::value_type, vint::max_length> encoding_buffer;
    return vint::serialize(val, encoding_buffer.begin());
}

temporary_buffer<char> serialize_vint(vint::value_type value) {
    std::array<bytes::value_type, vint::max_length> encoding_buffer;
    const auto size = vint::serialize(value, encoding_buffer.begin());
    return temporary_buffer<char>(
      reinterpret_cast<const char*>(encoding_buffer.data()), size);
}
} // namespace internal

inline std::random_device::result_type get_seed() {
    std::random_device rd;
    auto seed = rd();
    std::cout << "storage::random_batch seed = " << seed << "\n";
    return seed;
}

static thread_local std::default_random_engine gen(get_seed());

inline std::uniform_int_distribution<int> bool_dist{0, 1};
inline std::uniform_int_distribution<int> low_count_dist{2, 30};
inline std::uniform_int_distribution<int> high_count_dist{1024, 4096};
inline std::uniform_int_distribution<model::timestamp::value_type>
  timestamp_dist{model::timestamp::min().value(),
                 model::timestamp::min().value() + 2};

model::record_batch_header
make_random_header(model::offset o, model::timestamp ts, size_t num_records) {
    model::record_batch_header h;
    h.base_offset = o;
    h.last_offset_delta = num_records;
    h.first_timestamp = ts;
    h.type = model::record_batch_type(1);
    h.max_timestamp = model::timestamp(ts.value() + num_records);
    if (bool_dist(gen)) {
        h.attrs = model::record_batch_attributes(4);
    }
    return h;
}

temporary_buffer<char> make_buffer(size_t blob_size) {
    static thread_local std::
      independent_bits_engine<std::default_random_engine, 8, uint8_t>
        random_bytes;
    temporary_buffer<char> blob(blob_size);
    auto* out = blob.get_write();
    for (unsigned i = 0; i < blob_size; ++i) {
        *out++ = random_bytes();
    }
    return blob;
}

temporary_buffer<char> make_buffer_with_vint_size_prefix(size_t size) {
    auto buf = make_buffer(size + internal::vint_size(size));
    // overwrite the head of the buffer with the size prefix
    vint::serialize(size, reinterpret_cast<signed char*>(buf.get_write()));
    return buf;
}

fragbuf make_random_ftb(size_t blob_size) {
    auto first_chunk = blob_size / 2;
    auto second_chunk = blob_size - first_chunk;
    std::vector<temporary_buffer<char>> bufs;
    bufs.push_back(make_buffer(first_chunk));
    bufs.push_back(make_buffer(second_chunk));
    return fragbuf(std::move(bufs), first_chunk + second_chunk);
}

fragbuf make_packed_value_and_headers(size_t size) {
    std::vector<temporary_buffer<char>> bufs;

    // valueLen: varint
    // value: byte[]
    bufs.push_back(make_buffer_with_vint_size_prefix(size));

    // Headers => [Header]
    // numHeaders: varint
    int num_headers = low_count_dist(gen);
    bufs.push_back(internal::serialize_vint(num_headers));

    for (int i = 0; i < num_headers; i++) {
        // headerKeyLength: varint
        // headerKey: String
        sstring key = random_generators::gen_alphanum_string(
          low_count_dist(gen));
        bufs.push_back(internal::serialize_vint(key.size()));
        bufs.push_back(temporary_buffer<char>(
          reinterpret_cast<const char*>(key.data()), key.size()));

        // headerValueLength: varint
        // value: byte[]
        bufs.push_back(make_buffer_with_vint_size_prefix(low_count_dist(gen)));
    }

    return fragbuf(std::move(bufs));
}

model::record make_random_record(unsigned index) {
    auto k = make_random_ftb(high_count_dist(gen));
    auto v = make_packed_value_and_headers(high_count_dist(gen));
    auto size = internal::vint_size(k.size_bytes()) // size of key-len
                + k.size_bytes()                    // size of key
                + v.size_bytes()             // size of value (includes lengths)
                + internal::vint_size(index) // timestamp delta
                + internal::vint_size(index) // offset delta
                + sizeof(int8_t);            // attributes

    return model::record(
      size,
      model::record_attributes(0),
      index,
      index,
      std::move(k),
      std::move(v));
}

model::record_batch make_random_batch(model::offset o) {
    crc32 crc;
    auto num_records = low_count_dist(gen);
    auto ts = model::timestamp(timestamp_dist(gen));
    auto header = make_random_header(o, ts, num_records);
    storage::crc_batch_header(crc, header, num_records);
    auto size = packed_header_size;
    model::record_batch::records_type records;
    if (header.attrs.compression() != model::compression::none) {
        auto blob = make_random_ftb(high_count_dist(gen));
        size += blob.size_bytes();
        auto cr = model::record_batch::compressed_records(
          num_records, std::move(blob));
        crc.extend(cr.records());
        records = std::move(cr);
    } else {
        auto rs = model::record_batch::uncompressed_records();
        for (unsigned i = 0; i < num_records; ++i) {
            auto r = make_random_record(i);
            size += r.size_bytes();
            size += internal::vint_size(r.size_bytes());
            storage::crc_record_header_and_key(
              crc,
              r.size_bytes(),
              r.attributes(),
              r.timestamp_delta(),
              r.offset_delta(),
              r.key());
            crc.extend(r.packed_value_and_headers());
            rs.push_back(std::move(r));
        }
        records = std::move(rs);
    }
    header.size_bytes = size;
    header.crc = crc.value();
    return model::record_batch(std::move(header), std::move(records));
}

std::vector<model::record_batch>
make_random_batches(model::offset o, size_t count) { // start offset + count
    std::vector<model::record_batch> ret;
    ret.reserve(count);
    for (size_t i = 0; i < count; i++) {
        auto b = make_random_batch(o);
        o = b.last_offset() + 1;
        ret.push_back(std::move(b));
    }
    return ret;
}

std::vector<model::record_batch>
make_random_batches(model::offset o = model::offset(0)) {
    return make_random_batches(o, low_count_dist(gen));
}

} // namespace storage::test
