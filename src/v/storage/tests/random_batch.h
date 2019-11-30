#pragma once

#include "bytes/iobuf.h"
#include "hashing/crc32c.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "random/generators.h"
#include "storage/constants.h"
#include "storage/crc_record.h"
#include "utils/vint.h"
// rand utils
#include "random/generators.h"

#include <random>

namespace storage::test {

namespace internal {
static size_t vint_size(size_t val) {
    std::array<bytes::value_type, vint::max_length> encoding_buffer;
    return vint::serialize(val, encoding_buffer.begin());
}

static temporary_buffer<char> serialize_vint(vint::value_type value) {
    std::array<bytes::value_type, vint::max_length> encoding_buffer;
    const auto size = vint::serialize(value, encoding_buffer.begin());
    return temporary_buffer<char>(
      reinterpret_cast<const char*>(encoding_buffer.data()), size);
}
} // namespace internal

static model::record_batch_header make_random_header(
  model::offset o,
  model::timestamp ts,
  size_t num_records,
  bool allow_compression) {
    model::record_batch_header h;
    h.base_offset = o;
    h.last_offset_delta = num_records - 1;
    h.first_timestamp = ts;
    h.type = model::record_batch_type(1);
    h.max_timestamp = model::timestamp(ts.value() + num_records);
    if (allow_compression && random_generators::get_int(0, 1)) {
        h.attrs = model::record_batch_attributes(4);
    }
    return h;
}

static temporary_buffer<char> make_buffer(size_t blob_size) {
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

static temporary_buffer<char> make_buffer_with_vint_size_prefix(size_t size) {
    auto buf = make_buffer(size + internal::vint_size(size));
    // overwrite the head of the buffer with the size prefix
    vint::serialize(size, reinterpret_cast<signed char*>(buf.get_write()));
    return buf;
}

static iobuf make_random_ftb(size_t blob_size) {
    auto first_chunk = blob_size / 2;
    auto second_chunk = blob_size - first_chunk;
    iobuf b;
    b.append(make_buffer(first_chunk));
    b.append(make_buffer(second_chunk));
    return b;
}

static iobuf make_packed_value_and_headers(size_t size) {
    std::vector<temporary_buffer<char>> bufs;

    // valueLen: varint
    // value: byte[]
    bufs.push_back(make_buffer_with_vint_size_prefix(size));

    // Headers => [Header]
    // numHeaders: varint
    int num_headers = random_generators::get_int(2, 30);
    bufs.push_back(internal::serialize_vint(num_headers));

    for (int i = 0; i < num_headers; i++) {
        // headerKeyLength: varint
        // headerKey: String
        sstring key = random_generators::gen_alphanum_string(
          random_generators::get_int(2, 30));
        bufs.push_back(internal::serialize_vint(key.size()));
        bufs.push_back(temporary_buffer<char>(
          reinterpret_cast<const char*>(key.data()), key.size()));

        // headerValueLength: varint
        // value: byte[]
        bufs.push_back(
          make_buffer_with_vint_size_prefix(random_generators::get_int(2, 30)));
    }

    return iobuf(std::move(bufs));
}

static model::record make_random_record(unsigned index) {
    auto k = make_random_ftb(random_generators::get_int(1024, 4096));
    auto v = make_packed_value_and_headers(
      random_generators::get_int(1024, 4096));
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

static model::record_batch
make_random_batch(model::offset o, size_t num_records, bool allow_compression) {
    crc32 crc;
    auto ts = model::timestamp(
      random_generators::get_int<model::timestamp::value_type>(
        model::timestamp::min().value(), model::timestamp::min().value() + 2));
    auto header = make_random_header(o, ts, num_records, allow_compression);
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

static model::record_batch
make_random_batch(model::offset o, bool allow_compression = true) {
    auto num_records = random_generators::get_int(2, 30);
    return make_random_batch(o, num_records, allow_compression);
}

static std::vector<model::record_batch> make_random_batches(
  model::offset o,
  size_t count,
  bool allow_compression = true) { // start offset + count
    std::vector<model::record_batch> ret;
    ret.reserve(count);
    for (size_t i = 0; i < count; i++) {
        auto b = make_random_batch(o, allow_compression);
        o = b.last_offset() + model::offset(1);
        ret.push_back(std::move(b));
    }
    return ret;
}

static std::vector<model::record_batch>
make_random_batches(model::offset o = model::offset(0)) {
    return make_random_batches(o, random_generators::get_int(2, 30));
}

} // namespace storage::test
