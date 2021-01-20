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

#include "bytes/iobuf.h"
#include "hashing/secure.h"
#include "raft/types.h"
#include "random/generators.h"
#include "storage/record_batch_builder.h"

model::record_header create_wasm_header(ss::sstring key, ss::sstring value) {
    iobuf hkey, hval;
    const auto key_size = key.size();
    const auto val_size = value.size();
    hkey.append(key.data(), key_size);
    hval.append(value.data(), val_size);
    return model::record_header(
      key_size, std::move(hkey), val_size, std::move(hval));
}

/// Generates an event with randomized data fields, optionally bounded by event
/// type
model::record create_wasm_record(wasm_event e) {
    iobuf key, value;
    std::vector<model::record_header> headers;
    if (e.uuid) {
        key.append(e.uuid->data(), e.uuid->length());
    }
    if (e.script) {
        value.append(e.script->data(), e.script->length());
    }
    if (e.desc) {
        headers.emplace_back(
          create_wasm_header("description", std::move(*e.desc)));
    }
    if (e.checksum) {
        headers.emplace_back(
          create_wasm_header("checksum", std::move(*e.checksum)));
    }
    if (e.action) {
        ss::sstring action_value = (*e.action
                                    == coproc::wasm_event_action::deploy)
                                     ? "deploy"
                                     : "remove";
        headers.emplace_back(
          create_wasm_header("action", std::move(action_value)));
    }
    /// TODO(rob) a bit of code duplication with
    /// storage/test/utils/random_batch.cc
    auto k_z = key.size_bytes();
    auto v_z = value.size_bytes();
    auto size = sizeof(model::record_attributes::type) // attributes
                + vint::vint_size(0)                   // timestamp delta
                + vint::vint_size(0)                   // offset delta
                + vint::vint_size(k_z)                 // size of key-len
                + key.size_bytes()                     // size of key
                + vint::vint_size(v_z)                 // size of value
                + value.size_bytes() // size of value (includes lengths)
                + vint::vint_size(headers.size());
    for (auto& h : headers) {
        size += vint::vint_size(h.key_size()) + h.key().size_bytes()
                + vint::vint_size(h.value_size()) + h.value().size_bytes();
    }
    return model::record(
      size,
      model::record_attributes(0),
      0,
      0,
      k_z,
      std::move(key),
      v_z,
      std::move(value),
      std::move(headers));
}

model::record gen_valid_wasm_event(make_event_type type) {
    auto uuid = random_generators::gen_alphanum_string(15);
    if (type == make_event_type::random) {
        type = (random_generators::get_int(0, 1) == 0)
                 ? make_event_type::deploy
                 : make_event_type::remove;
    }
    if (type == make_event_type::remove) {
        return create_wasm_record(wasm_event{
          .uuid = std::move(uuid),
          .action = coproc::wasm_event_action::remove});
    }
    auto script = random_generators::gen_alphanum_string(15);
    hash_sha256 h;
    h.update(script);
    auto checksum = to_hex(h.reset());
    return create_wasm_record(wasm_event{
      .uuid = std::move(uuid),
      .desc = random_generators::gen_alphanum_string(15),
      .script = std::move(script),
      .checksum = checksum,
      .action = coproc::wasm_event_action::deploy});
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
    auto num_records = random_generators::get_int(2, 30);
    std::vector<model::record> wasm_recs;
    wasm_recs.reserve(num_records);
    for (int i = 0; i < num_records; ++i) {
        wasm_recs.emplace_back(gen_valid_wasm_event());
    }
    return make_wasm_batch(o, std::move(wasm_recs));
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
