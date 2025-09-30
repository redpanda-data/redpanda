// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/tests/random_batch.h"

#include "base/vassert.h"
#include "bytes/iobuf.h"
#include "model/batch_compression.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/record_utils.h"
#include "random/generators.h"
#include "test_utils/random_bytes.h"
#include "utils/vint.h"

#include <seastar/core/future.hh>
#include <seastar/core/smp.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <random>
#include <vector>

namespace model::test {
using namespace tests; // NOLINT

namespace {
std::vector<model::record_header> make_headers(int n = 2) {
    std::vector<model::record_header> ret;
    ret.reserve(n);
    for (int i = 0; i < n; ++i) {
        int key_len = get_int(i, 10);
        int val_len = get_int(i, 10);
        ret.emplace_back(
          key_len, random_iobuf(key_len), val_len, random_iobuf(val_len));
    }
    return ret;
}

struct record_spec {
    int index;
    std::optional<iobuf> key;
    std::optional<size_t> record_size;
    int num_headers = 2;
};

model::record make_random_record(record_spec spec) {
    iobuf k;
    vassert(
      !spec.key || !spec.record_size,
      "Cannot specify both key and record size.");
    if (spec.key) {
        k = std::move(*spec.key);
    } else if (spec.record_size) {
        k = random_iobuf(*spec.record_size);
    } else {
        k = random_iobuf();
    }
    auto k_z = k.size_bytes();
    auto v = random_iobuf();
    auto v_z = v.size_bytes();
    auto headers = make_headers(spec.num_headers);
    auto size = sizeof(model::record_attributes::type) // attributes
                + vint::vint_size(spec.index)          // timestamp delta
                + vint::vint_size(spec.index)          // offset delta
                + vint::vint_size(k_z)                 // size of key-len
                + k.size_bytes()                       // size of key
                + vint::vint_size(v_z)                 // size of value
                + v.size_bytes() // size of value (includes lengths)
                + vint::vint_size(headers.size());
    for (auto& h : headers) {
        size += vint::vint_size(h.key_size()) + h.key().size_bytes()
                + vint::vint_size(h.value_size()) + h.value().size_bytes();
    }
    return {
      static_cast<int32_t>(size),
      model::record_attributes(0),
      spec.index,
      spec.index,
      static_cast<int32_t>(k_z),
      std::move(k),
      static_cast<int32_t>(v_z),
      std::move(v),
      std::move(headers)};
}
} // namespace

model::record make_random_record(int index, iobuf key) {
    return make_random_record({.index = index, .key = std::move(key)});
}

model::record_batch make_random_batch(
  model::offset o,
  int num_records,
  bool allow_compression,
  model::record_batch_type bt,
  std::optional<std::vector<size_t>> record_sizes,
  std::optional<model::timestamp> ts) {
    return make_random_batch(
      record_batch_spec{
        .offset = o,
        .allow_compression = allow_compression,
        .count = num_records,
        .bt = bt,
        .record_sizes = std::move(record_sizes),
        .timestamp = ts});
}

model::record_batch
make_random_batch(model::offset o, int num_records, bool allow_compression) {
    return make_random_batch(
      o,
      num_records,
      allow_compression,
      model::record_batch_type::raft_data,
      std::nullopt);
}

model::record_batch make_random_batch(record_batch_spec spec) {
    auto ts = spec.timestamp.value_or(model::timestamp::now());
    auto max_ts = ts;
    if (!spec.all_records_have_same_timestamp) {
        max_ts = model::timestamp(ts() + spec.count - 1);
    }
    auto header = model::record_batch_header{
      .size_bytes = 0, // computed later
      .base_offset = spec.offset,
      .type = spec.bt,
      .crc = 0, // we-reassign later
      .attrs = {},
      .last_offset_delta = spec.count - 1,
      .first_timestamp = ts,
      .max_timestamp = max_ts,
      .producer_id = spec.producer_id,
      .producer_epoch = spec.producer_epoch,
      .base_sequence = 0,
      .record_count = spec.count};

    if (spec.is_transactional) {
        header.attrs.set_transactional_type();
    }

    if (spec.enable_idempotence) {
        header.base_sequence = spec.base_sequence;
    }

    model::record_batch::records_type records;
    auto rs = model::record_batch::uncompressed_records();
    rs.reserve(spec.count);
    for (int i = 0; i < spec.count; ++i) {
        std::optional<size_t> sz = std::nullopt;
        if (spec.record_sizes) {
            sz = spec.record_sizes->at(i);
        }
        if (spec.max_key_cardinality) {
            auto keystr = gen_alphanum_max_distinct(*spec.max_key_cardinality);
            auto key = iobuf::from(keystr.c_str());
            rs.emplace_back(make_random_record({
              .index = i,
              .key = std::move(key),
              .num_headers = spec.headers_per_record,
            }));
        } else {
            rs.emplace_back(make_random_record({
              .index = i,
              .record_size = sz,
              .num_headers = spec.headers_per_record,
            }));
        }
    }
    iobuf body;
    for (auto& r : rs) {
        model::append_record_to_buffer(body, r);
    }
    rs.clear();
    // TODO: expose term setting
    header.ctx = model::record_batch_header::context(
      model::term_id(0), ss::this_shard_id());
    header.size_bytes = static_cast<int32_t>(
      model::packed_record_batch_header_size + body.size_bytes());
    auto batch = model::record_batch(
      header, std::move(body), model::record_batch::tag_ctor_ng{});
    auto compression = static_cast<model::compression>(
      get_int<uint8_t>(0, spec.allow_compression ? 4 : 0));
    if (compression == model::compression::none) {
        batch.header().reset_size_checksum_metadata(batch.data());
        return batch;
    }
    return model::compress_batch_sync(compression, std::move(batch));
}

model::record_batch make_random_batch(
  model::offset o,
  bool allow_compression,
  std::optional<model::timestamp> ts,
  int records_per_batch) {
    auto num_records = records_per_batch > 0 ? records_per_batch
                                             : get_int(2, 30);
    return make_random_batch(
      o,
      num_records,
      allow_compression,
      model::record_batch_type::raft_data,
      std::nullopt,
      ts);
}

ss::future<chunked_circular_buffer<model::record_batch>> make_random_batches(
  model::offset o,
  int count,
  bool allow_compression,
  std::optional<model::timestamp> base_ts,
  int records_per_batch) {
    // start offset + count
    chunked_circular_buffer<model::record_batch> ret;
    model::timestamp ts = base_ts.value_or(model::timestamp::now());
    for (int i = 0; i < count; i++) {
        // TODO: it looks like a bug: make_random_batch adds
        // random number of records like we increment offset
        // always by one
        auto b = make_random_batch(o, allow_compression, ts, records_per_batch);
        o = b.last_offset() + model::offset(1);
        b.set_term(model::term_id(0));
        ret.push_back(std::move(b));
        if (i % 5 == 0) {
            co_await ss::coroutine::maybe_yield();
        }
    }
    co_return ret;
}

ss::future<chunked_circular_buffer<model::record_batch>>
make_random_batches(model::offset o) {
    return make_random_batches(o, get_int(2, 30), true);
}

ss::future<chunked_circular_buffer<model::record_batch>>
make_random_batches(record_batch_spec spec) {
    // start offset + count
    chunked_circular_buffer<model::record_batch> ret;
    model::offset o = spec.offset;
    int32_t base_sequence = spec.base_sequence;
    model::timestamp ts = spec.timestamp.value_or(model::timestamp::now());
    for (int i = 0; i < spec.count; i++) {
        auto num_records = spec.records ? *spec.records : get_int(2, 30);
        auto batch_spec = spec;
        batch_spec.timestamp = ts;
        if (!batch_spec.all_records_have_same_timestamp) {
            ts = model::timestamp(ts() + num_records);
        }
        batch_spec.offset = o;
        batch_spec.count = num_records;
        if (spec.enable_idempotence) {
            batch_spec.base_sequence = base_sequence;
            base_sequence += num_records;
        }
        auto b = make_random_batch(batch_spec);
        o = b.last_offset() + model::offset(1);
        b.set_term(model::term_id(0));
        ret.push_back(std::move(b));
        if (i % 5 == 0) {
            co_await ss::coroutine::maybe_yield();
        }
    }
    co_return ret;
}
} // namespace model::test
