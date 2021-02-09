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
#include "model/record_batch_reader.h"
#include "raft/types.h"
#include "random/generators.h"
#include "storage/record_batch_builder.h"

namespace coproc::wasm {

model::record_header
create_header(const ss::sstring& key, const ss::sstring& value) {
    iobuf hkey, hval;
    const auto key_size = key.size();
    const auto val_size = value.size();
    hkey.append(key.data(), key_size);
    hval.append(value.data(), val_size);
    return model::record_header(
      key_size, std::move(hkey), val_size, std::move(hval));
}

model::record_header create_header(const ss::sstring& key, const bytes& value) {
    iobuf hkey;
    const auto key_size = key.size();
    const auto val_size = value.size();
    hkey.append(key.data(), key_size);
    iobuf hval = bytes_to_iobuf(value);
    return model::record_header(
      key_size, std::move(hkey), val_size, std::move(hval));
}

void serialize_event(storage::record_batch_builder& rbb, const event& e) {
    iobuf key, value;
    std::vector<model::record_header> headers;
    if (e.name) {
        key.append(e.name->data(), e.name->length());
    }
    if (e.script) {
        value.append(e.script->data(), e.script->length());
    }
    if (e.desc) {
        headers.emplace_back(create_header("description", *e.desc));
    }
    if (e.checksum) {
        headers.emplace_back(create_header("sha256", *e.checksum));
    }
    if (e.action) {
        auto action_to_str = [](event_action action) -> ss::sstring {
            return (action == event_action::deploy) ? "deploy" : "remove";
        };
        headers.emplace_back(create_header("action", action_to_str(*e.action)));
    }
    rbb.add_raw_kw(std::move(key), std::move(value), std::move(headers));
}

bytes calculate_checksum(const event& e) {
    hash_sha256 h;
    h.update(*e.script);
    auto checksum = h.reset();
    return bytes(checksum.begin(), checksum.end());
}

/// Don't call this in a loop, rather use 'make_random_wasm_batch' or
/// 'make_wasm_batch'. This method is only useful for situations where a single
/// model::record with exact fields must be serialized
model::record make_record(const event& e) {
    storage::record_batch_builder rbb(raft::data_batch_type, model::offset(0));
    serialize_event(rbb, e);
    auto record_batch = std::move(rbb).build();
    vassert(
      record_batch.header().record_count == 1, "Only one record expected");
    return std::move(record_batch.copy_records()[0]);
}

model::record_batch_reader make_random_event_record_batch_reader(
  model::offset offset, int batch_size, int n_batches) {
    model::record_batch_reader::data_t batches;
    model::offset o{offset};
    for (auto i = 0; i < n_batches; ++i) {
        storage::record_batch_builder rbb(raft::data_batch_type, o);
        for (int j = 0; j < batch_size; ++j) {
            event e{
              .name = random_generators::gen_alphanum_string(15),
              .action = event_action::remove};
            if (random_generators::get_int(0, 1) == 0) {
                e.action = event_action::deploy;
                e.script = random_generators::gen_alphanum_string(15);
                e.desc = random_generators::gen_alphanum_string(15);
                e.checksum = calculate_checksum(e);
            }
            serialize_event(rbb, e);
        }
        batches.push_back(std::move(rbb).build());
        o = batches.back().last_offset() + model::offset(1);
    }
    return model::make_memory_record_batch_reader(std::move(batches));
}

model::record_batch_reader make_event_record_batch_reader(
  std::vector<std::vector<short_event>> event_batches) {
    model::record_batch_reader::data_t batches;
    model::offset o{0};
    for (const auto& events : event_batches) {
        storage::record_batch_builder rbb(raft::data_batch_type, o);
        for (const short_event& se : events) {
            coproc::wasm::event e{.name = se.name, .action = se.action};
            if (e.action == event_action::deploy) {
                e.desc = random_generators::gen_alphanum_string(15);
                e.script = random_generators::gen_alphanum_string(15);
                e.checksum = calculate_checksum(e);
            }
            serialize_event(rbb, e);
        }
        batches.push_back(std::move(rbb).build());
        o = batches.back().last_offset() + model::offset(1);
    }
    return model::make_memory_record_batch_reader(std::move(batches));
}

} // namespace coproc::wasm
