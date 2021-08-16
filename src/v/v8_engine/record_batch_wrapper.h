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
#include "v8.h"

#include <seastar/core/temporary_buffer.hh>

#include <string_view>
#include <v8-internal.h>

namespace v8_engine {

ss::temporary_buffer<char> get_buf(iobuf buf);

struct record_wrapper {
    record_wrapper() = default;
    explicit record_wrapper(model::record record)
      : key(get_buf(record.release_key()))
      , value(get_buf(record.release_value()))
      , size_bytes(record.size_bytes())
      , offset(record.offset_delta())
      , timestamp(record.timestamp_delta()) {}

    ss::temporary_buffer<char> key;
    ss::temporary_buffer<char> value;
    int32_t size_bytes{};
    int32_t offset{};
    int64_t timestamp{};
};

struct record_batch_wrapper {
    explicit record_batch_wrapper(model::record_batch& batch);

    model::record_batch get_output_batch();

    static void size(const v8::FunctionCallbackInfo<v8::Value>& args);

    static void consume(const v8::FunctionCallbackInfo<v8::Value>& args);

    static void produce(const v8::FunctionCallbackInfo<v8::Value>& args);

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

    std::list<record_wrapper> input_records;
    std::list<record_wrapper>::iterator input_records_it;

    std::list<record_wrapper> output_records;

    model::record_batch_header header;
};

} // namespace v8_engine
