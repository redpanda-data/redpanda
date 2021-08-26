/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "model/record.h"
#include "model/record_utils.h"
#include "v8.h"

#include <seastar/core/temporary_buffer.hh>

#include <string_view>
#include <v8-internal.h>

namespace v8_engine {

struct record_wrapper {
    record_wrapper() = default;
    explicit record_wrapper(model::record record)
      : size_bytes(record.size_bytes())
      , offset(record.offset_delta())
      , timestamp(record.timestamp_delta()) {
        if (record.key_size() > 0) {
            auto raw_key = iobuf_const_parser(record.value())
                             .read_bytes(record.key_size());
            key = ss::temporary_buffer<char>(
              reinterpret_cast<char*>(raw_key.data()), raw_key.size());
        }
        if (record.value_size() > 0) {
            auto raw_value = iobuf_const_parser(record.value())
                               .read_bytes(record.value_size());
            value = ss::temporary_buffer<char>(
              reinterpret_cast<char*>(raw_value.data()), raw_value.size());
        }
    }

    int32_t size_bytes{};
    int32_t offset{};
    int64_t timestamp{};

    ss::temporary_buffer<char> key{};
    ss::temporary_buffer<char> value{};
};

model::record_batch convert_to_record_batch(
  model::record_batch_header header, std::list<record_wrapper>& records);

// Record batch wrapper is used in js script. User has input object with 3
// methods: consume, produce, size. It contains list with input records and
// output records.
struct record_batch_wrapper {
    explicit record_batch_wrapper(model::record_batch& batch);

    // Return size for input batch
    static void size(const v8::FunctionCallbackInfo<v8::Value>& args);

    // Consume one record from input topic
    static void consume(const v8::FunctionCallbackInfo<v8::Value>& args);

    // Produce one record to output object
    static void produce(const v8::FunctionCallbackInfo<v8::Value>& args);

    model::record_batch get_output_batch();

    model::record_batch_header header;
    std::list<record_wrapper> input_records;
    std::list<record_wrapper>::iterator input_records_it;

    std::list<record_wrapper> output_records;

private:
    v8::Local<v8::Map> set_record(v8::Isolate* isolate);

    v8::Local<v8::Map> set_field(
      const std::string_view key,
      v8::Local<v8::Map>& map,
      v8::Isolate* isolate);

    void get_record(v8::Map* map, v8::Isolate* isolate);

    bool
    check_key_in_map(v8::Map* map, v8::Isolate* isolate, std::string_view key);

    void get_field(
      const std::string_view key,
      v8::Map* map,
      v8::Isolate* isolate,
      record_wrapper& new_record);

    static void throw_exception(v8::Isolate* isolate, std::string_view msg) {
        isolate->ThrowError(
          v8::String::NewFromUtf8(isolate, msg.data()).ToLocalChecked());
    }
};

} // namespace v8_engine
