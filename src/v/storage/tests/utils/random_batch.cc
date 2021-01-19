// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/tests/utils/random_batch.h"

#include "bytes/iobuf.h"
#include "compression/compression.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/record_utils.h"
#include "random/generators.h"
#include "storage/parser_utils.h"
#include "utils/vint.h"

#include <seastar/core/future.hh>
#include <seastar/core/smp.hh>

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

model::record_batch make_random_batch(
  model::offset o,
  int num_records,
  bool allow_compression,
  model::record_batch_type bt) {
    auto ts = model::timestamp::now()() - (num_records - 1);
    auto header = model::record_batch_header{
      .size_bytes = 0, // computed later
      .base_offset = o,
      .type = bt,
      .crc = 0, // we-reassign later
      .attrs = model::record_batch_attributes(
        get_int<int16_t>(0, allow_compression ? 4 : 0)),
      .last_offset_delta = num_records - 1,
      .first_timestamp = model::timestamp(ts),
      .max_timestamp = model::timestamp(ts + num_records - 1),
      .producer_id = 0,
      .producer_epoch = 0,
      .base_sequence = 0,
      .record_count = num_records};

    auto size = model::packed_record_batch_header_size;
    model::record_batch::records_type records;
    auto rs = model::record_batch::uncompressed_records();
    rs.reserve(num_records);
    for (int i = 0; i < num_records; ++i) {
        rs.emplace_back(make_random_record(i));
    }
    if (header.attrs.compression() != model::compression::none) {
        iobuf body;
        for (auto& r : rs) {
            model::append_record_to_buffer(body, r);
        }
        rs.clear();
        records = compression::compressor::compress(
          body, header.attrs.compression());
        size += std::get<iobuf>(records).size_bytes();
    } else {
        for (auto& r : rs) {
            size += r.size_bytes();
            size += vint::vint_size(r.size_bytes());
        }
        records = std::move(rs);
    }
    // TODO: expose term setting
    header.ctx = model::record_batch_header::context(
      model::term_id(0), ss::this_shard_id());
    header.size_bytes = size;
    auto batch = model::record_batch(header, std::move(records));
    batch.header().crc = model::crc_record_batch(batch);
    batch.header().header_crc = model::internal_header_only_crc(batch.header());
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
    for (int i = 0; i < count; i++) {
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

model::record_batch_reader make_random_memory_record_batch_reader(
  model::offset offset, int batch_size, int n_batches, bool allow_compression) {
    return model::make_generating_record_batch_reader(
      [offset, batch_size, n_batches, allow_compression]() mutable {
          model::record_batch_reader::data_t batches;
          if (n_batches--) {
              batches = make_random_batches(
                offset, batch_size, allow_compression);
              offset = batches.back().last_offset()++;
          }
          return ss::make_ready_future<model::record_batch_reader::data_t>(
            std::move(batches));
      });
}

} // namespace storage::test
