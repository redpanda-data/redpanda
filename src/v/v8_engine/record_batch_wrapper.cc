/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "v8_engine/record_batch_wrapper.h"

namespace v8_engine {

ss::temporary_buffer<char> get_buf(iobuf buf) {
    ss::temporary_buffer<char> temp_buf(buf.size_bytes());
    char* buf_ptr = temp_buf.get_write();
    for (auto& part : buf) {
        std::memcpy(buf_ptr, part.get_write(), part.size());
        buf_ptr += part.size();
    }

    return temp_buf;
}

record_batch_wrapper::record_batch_wrapper(model::record_batch& batch)
  : header(batch.header()) {
    batch.for_each_record(
      [this](auto record) { input_records.emplace_back(std::move(record)); });
    input_records_it = input_records.begin();
}

model::record_batch record_batch_wrapper::get_output_batch() {
    iobuf serialize_reords;
    for (auto& r : output_records) {
        iobuf key;
        auto key_size = r.key.size();
        key.append(std::move(r.key));
        iobuf value;
        auto value_size = r.value.size();
        value.append(std::move(r.value));

        static constexpr size_t zero_vint_size = vint::vint_size(0);
        auto size = sizeof(model::record_attributes::type) // attributes
                    + vint::vint_size(r.timestamp)         // timestamp delta
                    + vint::vint_size(r.offset)            // offset_delta
                    + vint::vint_size(key.size_bytes())    // key size
                    + key.size_bytes()                     // key payload
                    + vint::vint_size(value.size_bytes())  // value size
                    + value.size_bytes() + zero_vint_size;

        model::record new_record(
          size,
          {},
          r.timestamp,
          r.offset,
          key_size == 0 ? -1 : key_size,
          std::move(key),
          value_size,
          std::move(value),
          {});

        model::append_record_to_buffer(serialize_reords, new_record);
    }

    header.size_bytes = serialize_reords.size_bytes()
                        + model::packed_record_batch_header_size;
    header.record_count = output_records.size();
    header.crc = model::crc_record_batch(header, serialize_reords);
    model::record_batch new_batch(header, std::move(serialize_reords));
    return new_batch;
}

void record_batch_wrapper::size(
  const v8::FunctionCallbackInfo<v8::Value>& args) {
    auto isolate = args.GetIsolate();
    if (args.Length() != 0) {
        throw_exception(isolate, "Invalid number of args for size method");
        return;
    }

    v8::Local<v8::Object> self = args.Holder();
    v8::Local<v8::External> wrap = v8::Local<v8::External>::Cast(
      self->GetInternalField(0));
    auto record_batch_ptr = static_cast<record_batch_wrapper*>(wrap->Value());

    args.GetReturnValue().Set(
      static_cast<uint32_t>(record_batch_ptr->input_records.size()));
}

void record_batch_wrapper::consume(
  const v8::FunctionCallbackInfo<v8::Value>& args) {
    auto isolate = args.GetIsolate();
    if (args.Length() != 0) {
        throw_exception(isolate, "Invalid number of args for consume method");
        return;
    }
    v8::Local<v8::Object> self = args.Holder();
    v8::Local<v8::External> wrap = v8::Local<v8::External>::Cast(
      self->GetInternalField(0));
    auto record_batch_ptr = static_cast<record_batch_wrapper*>(wrap->Value());

    if (
      record_batch_ptr->input_records_it
      == record_batch_ptr->input_records.end()) {
        throw_exception(isolate, "Try to consume out of range");
        return;
    }

    auto map = record_batch_ptr->set_record(isolate);

    record_batch_ptr->input_records_it++;

    args.GetReturnValue().Set(map);
}

void record_batch_wrapper::produce(
  const v8::FunctionCallbackInfo<v8::Value>& args) {
    auto isolate = args.GetIsolate();
    if (args.Length() != 1) {
        throw_exception(isolate, "Invalid number of args for produce method");
        return;
    }

    auto value = args[0];
    if (!value->IsMap()) {
        throw_exception(isolate, "Argument for produce must be Map");
    }

    v8::Local<v8::Object> self = args.Holder();
    v8::Local<v8::External> wrap = v8::Local<v8::External>::Cast(
      self->GetInternalField(0));
    auto record_batch_ptr = static_cast<record_batch_wrapper*>(wrap->Value());

    auto map = v8::Map::Cast(*value);
    record_batch_ptr->get_record(map, isolate);
}

v8::Local<v8::Map> record_batch_wrapper::set_record(v8::Isolate* isolate) {
    auto map = v8::Map::New(isolate);
    map = set_field("key", map, isolate);
    map = set_field("value", map, isolate);
    map = set_field("size_bytes", map, isolate);
    map = set_field("offset", map, isolate);
    map = set_field("timestamp", map, isolate);
    return map;
}

v8::Local<v8::Map> record_batch_wrapper::set_field(
  const std::string_view key, v8::Local<v8::Map>& map, v8::Isolate* isolate) {
    if (key == "key" || key == "value") {
        char* data = nullptr;
        size_t length = 0;
        if (key == "key") {
            data = input_records_it->key.get_write();
            length = input_records_it->key.size();
        }

        if (key == "value") {
            data = input_records_it->value.get_write();
            length = input_records_it->value.size();
        }

        auto store = v8::ArrayBuffer::NewBackingStore(
          data, length, v8::BackingStore::EmptyDeleter, nullptr);

        auto array_buf = v8::ArrayBuffer::New(isolate, std::move(store));

        return map
          ->Set(
            isolate->GetCurrentContext(),
            v8::String::NewFromUtf8(isolate, key.data()).ToLocalChecked(),
            array_buf)
          .ToLocalChecked();
    }

    if (key == "timestamp" || key == "offset" || key == "size_bytes") {
        int64_t value = 0;
        if (key == "timestamp") {
            value = input_records_it->timestamp;
        }
        if (key == "offset") {
            value = input_records_it->offset;
        }
        if (key == "size_bytes") {
            value = input_records_it->size_bytes;
        }

        return map
          ->Set(
            isolate->GetCurrentContext(),
            v8::String::NewFromUtf8(isolate, key.data()).ToLocalChecked(),
            v8::Integer::New(isolate, value))
          .ToLocalChecked();
    }
    return map;
}

void record_batch_wrapper::get_record(v8::Map* map, v8::Isolate* isolate) {
    if (!(check_key_in_map(map, isolate, "key")
          && check_key_in_map(map, isolate, "value")
          && check_key_in_map(map, isolate, "timestamp")
          && check_key_in_map(map, isolate, "offset")
          && check_key_in_map(map, isolate, "size_bytes"))) {
        throw_exception(
          isolate,
          "Can not find one of keys(key, value, timestamp, offset, "
          "size_bytes) in map for produce");
    }

    record_wrapper new_record;
    get_field("key", map, isolate, new_record);
    get_field("value", map, isolate, new_record);
    get_field("offset", map, isolate, new_record);
    get_field("timestamp", map, isolate, new_record);
    get_field("size_bytes", map, isolate, new_record);
    output_records.emplace_back(std::move(new_record));
}

bool record_batch_wrapper::check_key_in_map(
  v8::Map* map, v8::Isolate* isolate, std::string_view key) {
    return map
      ->Has(
        isolate->GetCurrentContext(),
        v8::String::NewFromUtf8(isolate, key.data()).ToLocalChecked())
      .ToChecked();
}

void record_batch_wrapper::get_field(
  const std::string_view key,
  v8::Map* map,
  v8::Isolate* isolate,
  record_wrapper& new_record) {
    if (key == "key" || key == "value") {
        auto data
          = map
              ->Get(
                isolate->GetCurrentContext(),
                v8::String::NewFromUtf8(isolate, key.data()).ToLocalChecked())
              .ToLocalChecked();
        auto array = v8::ArrayBuffer::Cast(*data);
        auto store = array->GetBackingStore();

        if (key == "key") {
            new_record.key = ss::temporary_buffer<char>(
              reinterpret_cast<char*>(store->Data()), store->ByteLength());
        }

        if (key == "value") {
            new_record.value = ss::temporary_buffer<char>(
              reinterpret_cast<char*>(store->Data()), store->ByteLength());
        }
    }

    if (key == "timestamp" || key == "offset" || key == "size_bytes") {
        auto data
          = map
              ->Get(
                isolate->GetCurrentContext(),
                v8::String::NewFromUtf8(isolate, key.data()).ToLocalChecked())
              .ToLocalChecked();
        auto integer = v8::Integer::Cast(*data);

        if (key == "timestamp") {
            new_record.timestamp = integer->Value();
        }
        if (key == "offset") {
            new_record.offset = integer->Value();
        }
        if (key == "size_bytes") {
            new_record.size_bytes = integer->Value();
        }
    }
}

} // namespace v8_engine
