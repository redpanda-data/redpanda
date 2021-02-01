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
    return make_random_batch(record_batch_spec{
      .offset = o,
      .allow_compression = allow_compression,
      .count = num_records,
      .bt = bt});
}

model::record_batch
make_random_batch(model::offset o, int num_records, bool allow_compression) {
    return make_random_batch(
      o, num_records, allow_compression, model::record_batch_type(1));
}

model::record_batch make_random_batch(record_batch_spec spec) {
    auto ts = model::timestamp::now()() - (spec.count - 1);
    auto header = model::record_batch_header{
      .size_bytes = 0, // computed later
      .base_offset = spec.offset,
      .type = spec.bt,
      .crc = 0, // we-reassign later
      .attrs = model::record_batch_attributes(
        get_int<int16_t>(0, spec.allow_compression ? 4 : 0)),
      .last_offset_delta = spec.count - 1,
      .first_timestamp = model::timestamp(ts),
      .max_timestamp = model::timestamp(ts + spec.count - 1),
      .producer_id = spec.producer_id,
      .producer_epoch = spec.producer_epoch,
      .base_sequence = 0,
      .record_count = spec.count};

    if (spec.enable_idempotence) {
        header.base_sequence = spec.base_sequence;
    }

    auto size = model::packed_record_batch_header_size;
    model::record_batch::records_type records;
    auto rs = model::record_batch::uncompressed_records();
    rs.reserve(spec.count);
    for (int i = 0; i < spec.count; ++i) {
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
    return make_random_batch(
      o, num_records, allow_compression, model::record_batch_type(1));
}

ss::circular_buffer<model::record_batch>
make_random_batches(model::offset o, int count, bool allow_compression) {
    // start offset + count
    ss::circular_buffer<model::record_batch> ret;
    ret.reserve(count);
    for (int i = 0; i < count; i++) {
        // TODO: it looks like a bug: make_random_batch adds
        // random number of records like we increment offset
        // always by one
        auto b = make_random_batch(o, allow_compression);
        o = b.last_offset() + model::offset(1);
        b.set_term(model::term_id(0));
        ret.push_back(std::move(b));
    }
    return ret;
}

ss::circular_buffer<model::record_batch> make_random_batches(model::offset o) {
    return make_random_batches(o, get_int(2, 30), true);
}

ss::circular_buffer<model::record_batch>
make_random_batches(record_batch_spec spec) {
    // start offset + count
    ss::circular_buffer<model::record_batch> ret;
    ret.reserve(spec.count);
    model::offset o = spec.offset;
    int32_t base_sequence = spec.base_sequence;
    for (int i = 0; i < spec.count; i++) {
        auto num_records = get_int(2, 30);
        auto batch_spec = spec;
        batch_spec.offset = o;
        batch_spec.count = num_records;
        if (spec.enable_idempotence) {
            batch_spec.base_sequence = base_sequence;
            base_sequence += num_records;
        }
        auto b = make_random_batch(batch_spec);
        o = b.last_offset() + model::offset(num_records);
        b.set_term(model::term_id(0));
        ret.push_back(std::move(b));
    }
    return ret;
}

model::record_batch_reader make_random_memory_record_batch_reader(
  model::offset offset, int batch_size, int n_batches, bool allow_compression) {
    return make_random_memory_record_batch_reader(
      record_batch_spec{
        .offset = offset,
        .allow_compression = allow_compression,
        .count = batch_size},
      n_batches);
}

model::record_batch_reader
make_random_memory_record_batch_reader(record_batch_spec spec, int n_batches) {
    return model::make_generating_record_batch_reader(
      [offset = spec.offset, spec, n_batches]() mutable {
          model::record_batch_reader::data_t batches;
          if (n_batches--) {
              auto batch_spec = spec;
              batch_spec.offset = offset;
              batches = make_random_batches(batch_spec);
              offset = batches.back().last_offset()++;
          }
          return ss::make_ready_future<model::record_batch_reader::data_t>(
            std::move(batches));
      });
}

} // namespace storage::test
