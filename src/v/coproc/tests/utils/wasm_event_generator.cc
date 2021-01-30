/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "coproc/tests/utils/wasm_event_generator.h"

#include "bytes/bytes.h"
#include "bytes/iobuf.h"
#include "coproc/wasm_event.h"
#include "hashing/secure.h"
#include "raft/types.h"
#include "random/generators.h"
#include "storage/record_batch_builder.h"

namespace coproc {

model::record_header
create_wasm_header(const ss::sstring& key, const ss::sstring& value) {
    iobuf hkey, hval;
    const auto key_size = key.size();
    const auto val_size = value.size();
    hkey.append(key.data(), key_size);
    hval.append(value.data(), val_size);
    return model::record_header(
      key_size, std::move(hkey), val_size, std::move(hval));
}

model::record_header
create_wasm_header(const ss::sstring& key, const bytes& value) {
    iobuf hkey;
    const auto key_size = key.size();
    const auto val_size = value.size();
    hkey.append(key.data(), key_size);
    iobuf hval = bytes_to_iobuf(value);
    return model::record_header(
      key_size, std::move(hkey), val_size, std::move(hval));
}

void serialize_wasm_event(
  storage::record_batch_builder& rbb, const wasm_event& e) {
    iobuf key, value;
    std::vector<model::record_header> headers;
    if (e.name) {
        key.append(e.name->data(), e.name->length());
    }
    if (e.script) {
        value.append(e.script->data(), e.script->length());
    }
    if (e.desc) {
        headers.emplace_back(create_wasm_header("description", *e.desc));
    }
    if (e.checksum) {
        headers.emplace_back(create_wasm_header("sha256", *e.checksum));
    }
    if (e.action) {
        auto action_to_str = [](wasm_event_action action) -> ss::sstring {
            return (action == wasm_event_action::deploy) ? "deploy" : "remove";
        };
        headers.emplace_back(
          create_wasm_header("action", action_to_str(*e.action)));
    }
    rbb.add_raw_kw(std::move(key), std::move(value), std::move(headers));
}

/// Don't call this in a loop, rather use 'make_random_wasm_batch' or
/// 'make_wasm_batch'. This method is only useful for situations where a single
/// model::record with exact fields must be serialized
model::record create_wasm_record(const wasm_event& e) {
    storage::record_batch_builder rbb(raft::data_batch_type, model::offset(0));
    serialize_wasm_event(rbb, e);
    auto record_batch = std::move(rbb).build();
    vassert(
      record_batch.header().record_count == 1, "Only one record expected");
    return std::move(record_batch.copy_records()[0]);
}

wasm_event make_random_wasm_event() {
    auto name = random_generators::gen_alphanum_string(15);
    if (random_generators::get_int(0, 1) == 0) {
        return wasm_event{
          .name = std::move(name), .action = wasm_event_action::remove};
    }
    auto script = random_generators::gen_alphanum_string(15);
    hash_sha256 h;
    h.update(script);
    auto checksum = h.reset();
    return wasm_event{
      .name = std::move(name),
      .desc = random_generators::gen_alphanum_string(15),
      .script = std::move(script),
      .checksum = bytes(checksum.begin(), checksum.end()),
      .action = wasm_event_action::deploy};
}

model::record_batch
make_wasm_batch(model::offset o, std::vector<model::record> events) {
    storage::record_batch_builder rbb(raft::data_batch_type, o);
    for (size_t i = 0; i < events.size(); ++i) {
        rbb.add_raw_kw(
          events[i].release_key(),
          events[i].release_value(),
          std::move(events[i].headers()));
    }
    return std::move(rbb).build();
}

model::record_batch make_random_wasm_batch(model::offset o) {
    storage::record_batch_builder rbb(raft::data_batch_type, o);
    auto num_records = random_generators::get_int(2, 30);
    for (int i = 0; i < num_records; ++i) {
        auto e = make_random_wasm_event();
        serialize_wasm_event(rbb, e);
    }
    return std::move(rbb).build();
}

model::record_batch_reader::data_t
make_random_wasm_batches(model::offset o, int count) {
    model::record_batch_reader::data_t batches;
    batches.reserve(count);
    for (int i = 0; i < count; ++i) {
        auto b = make_random_wasm_batch(o);
        b.set_term(model::term_id(0));
        batches.push_back(std::move(b));
    }
    return batches;
}

model::record_batch_reader make_wasm_event_record_batch_reader(
  model::offset offset, int batch_size, int n_batches) {
    return model::make_generating_record_batch_reader(
      [offset, batch_size, n_batches]() mutable {
          model::record_batch_reader::data_t batches;
          if (n_batches--) {
              batches = make_random_wasm_batches(offset, batch_size);
              offset = batches.back().last_offset()++;
          }
          return ss::make_ready_future<model::record_batch_reader::data_t>(
            std::move(batches));
      });
}

} // namespace coproc
