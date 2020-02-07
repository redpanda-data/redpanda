#include "storage/tests/utils/random_batch.h"

#include "bytes/iobuf.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/record_utils.h"
#include "random/generators.h"
#include "utils/vint.h"

#include <random>
#include <vector>

namespace storage::test {
using namespace random_generators; // NOLINT

iobuf make_iobuf(size_t n = 128) {
    const auto b = gen_alphanum_string(n);
    iobuf io;
    io.append(b.data(), n);
    return io;
}

std::vector<model::record_header> make_headers(int n = 2) {
    std::vector<model::record_header> ret;
    ret.reserve(n);
    for (int i = 0; i < n; ++i) {
        int key_len = get_int(i, 10);
        int val_len = get_int(i, 10);
        ret.emplace_back(model::record_header(
          key_len, make_iobuf(key_len), val_len, make_iobuf(val_len)));
    }
    return ret;
}

model::record make_random_record(int index) {
    auto k = make_iobuf();
    auto k_z = k.size_bytes();
    auto v = make_iobuf();
    auto v_z = v.size_bytes();
    auto headers = make_headers();
    auto size = sizeof(model::record_attributes::type) // attributes
                + vint::vint_size(index)               // timestamp delta
                + vint::vint_size(index)               // offset delta
                + vint::vint_size(k_z)                 // size of key-len
                + k.size_bytes()                       // size of key
                + vint::vint_size(v_z)                 // size of value
                + v.size_bytes() // size of value (includes lengths)
                + vint::vint_size(headers.size());
    for (auto& h : headers) {
        size += vint::vint_size(h.key_size()) + h.key().size_bytes()
                + vint::vint_size(h.value_size()) + h.value().size_bytes();
    }
    return model::record(
      size,
      model::record_attributes(0),
      index,
      index,
      k_z,
      std::move(k),
      v_z,
      std::move(v),
      std::move(headers));
}

model::record_batch
make_random_batch(model::offset o, int num_records, bool allow_compression) {
    auto ts = model::timestamp(get_int<model::timestamp::type>(
      model::timestamp::min().value(), model::timestamp::min().value() + 2));
    auto header = model::record_batch_header{
      .size_bytes = 0, // computed later
      .base_offset = o,
      .type = model::record_batch_type(1),
      .crc = 0, // we-reassign later
      .attrs = model::record_batch_attributes(
        get_int<int16_t>(0, allow_compression ? 4 : 0)),
      .last_offset_delta = num_records - 1,
      .first_timestamp = ts,
      .max_timestamp = ts,
      .producer_id = 0,
      .producer_epoch = 0,
      .base_sequence = 0,
      .record_count = num_records};

    auto size = model::packed_record_batch_header_size;
    model::record_batch::records_type records;
    if (header.attrs.compression() != model::compression::none) {
        auto blob = make_iobuf(get_int(1024, 4096));
        size += blob.size_bytes();
        records = std::move(blob);
    } else {
        auto rs = model::record_batch::uncompressed_records();
        for (int i = 0; i < num_records; ++i) {
            auto r = make_random_record(i);
            size += r.size_bytes();
            size += vint::vint_size(r.size_bytes());
            rs.push_back(std::move(r));
        }
        records = std::move(rs);
    }
    // TODO: expose term setting
    header.ctx.term = model::term_id(0);
    header.size_bytes = size;
    auto batch = model::record_batch(header, std::move(records));
    batch.header().crc = model::crc_record_batch(batch);
    return batch;
}

model::record_batch make_random_batch(model::offset o, bool allow_compression) {
    auto num_records = get_int(2, 30);
    return make_random_batch(o, num_records, allow_compression);
}

ss::circular_buffer<model::record_batch>
make_random_batches(model::offset o, int count, bool allow_compression) {
    // start offset + count
    ss::circular_buffer<model::record_batch> ret;
    ret.reserve(count);
    for (size_t i = 0; i < count; i++) {
        auto b = make_random_batch(o, allow_compression);
        o = b.last_offset() + model::offset(1);
        b.set_term(model::term_id(0));
        ret.push_back(std::move(b));
    }
    return ret;
}

ss::circular_buffer<model::record_batch> make_random_batches(model::offset o) {
    return make_random_batches(o, get_int(2, 30));
}

} // namespace storage::test
