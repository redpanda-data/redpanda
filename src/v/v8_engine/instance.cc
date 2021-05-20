/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "v8_engine/instance.h"

#include "utils/file_io.h"

#include <seastar/core/future.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/util/later.hh>

#include <utility>

namespace v8_engine {

instance::instance(size_t max_heap_size_in_bytes) {
    _create_params.array_buffer_allocator_shared
      = std::shared_ptr<v8::ArrayBuffer::Allocator>(
        v8::ArrayBuffer::Allocator::NewDefaultAllocator());
    _create_params.constraints.ConfigureDefaultsFromHeapSize(
      0, max_heap_size_in_bytes);
    _isolate = v8::Isolate::New(_create_params);
}

instance::~instance() {
    _function.Reset();
    _context.Reset();
    _isolate->Dispose();
}

ss::future<>
instance::init(ss::sstring name, ss::temporary_buffer<char> js_code) {
    return compile_script(std::move(js_code))
      .then([this, name = std::move(name)] {
          get_function(name);
          return ss::now();
      });
}

ss::future<> instance::compile_script(ss::temporary_buffer<char> js_code) {
    v8::Locker locker(_isolate);
    v8::Isolate::Scope isolate_scope(_isolate);
    v8::HandleScope handle_scope(_isolate);
    v8::TryCatch try_catch(_isolate);
    v8::Local<v8::Context> local_ctx = v8::Context::New(_isolate);
    v8::Context::Scope context_scope(local_ctx);

    v8::Local<v8::String> script_code
      = v8::String::NewFromUtf8(
          _isolate, js_code.begin(), v8::NewStringType::kNormal, js_code.size())
          .ToLocalChecked();
    v8::Local<v8::Script> compiled_script;
    if (!v8::Script::Compile(local_ctx, script_code)
           .ToLocal(&compiled_script)) {
        throw_exception_from_v8(try_catch, "Can not compile script");
    }

    // Run js code in first time and init some data from them.
    // Need to be running in executor.
    v8::Local<v8::Value> result;
    if (!compiled_script->Run(local_ctx).ToLocal(&result)) {
        throw_exception_from_v8(try_catch, "Can not run script in first time");
    }

    _context.Reset(_isolate, local_ctx);
    return ss::now();
}

void instance::get_function(std::string_view name) {
    v8::Locker locker(_isolate);
    v8::Isolate::Scope isolate_scope(_isolate);
    v8::HandleScope handle_scope(_isolate);
    v8::Local<v8::Context> local_ctx = v8::Local<v8::Context>::New(
      _isolate, _context);
    v8::Context::Scope context_scope(local_ctx);
    v8::Local<v8::String> function_name
      = v8::String::NewFromUtf8(_isolate, name.data()).ToLocalChecked();
    v8::Local<v8::Value> function_val;
    if (
      !local_ctx->Global()->Get(local_ctx, function_name).ToLocal(&function_val)
      || !function_val->IsFunction()) {
        throw instance_exception(
          fmt::format("Can not get function({}) from script", name));
    }

    _function.Reset(_isolate, function_val.As<v8::Function>());
}

// It is the first version for run. In next updates I use alient_thread for run
// v8.
ss::future<> instance::run(ss::temporary_buffer<char>& data) {
    return run_internal(data);
}

ss::future<> instance::run_internal(ss::temporary_buffer<char>& data) {
    v8::Locker locker(_isolate);
    v8::Isolate::Scope isolate_scope(_isolate);
    v8::HandleScope handle_scope(_isolate);
    v8::TryCatch try_catch(_isolate);
    v8::Local<v8::Context> local_ctx = v8::Local<v8::Context>::New(
      _isolate, _context);
    v8::Context::Scope context_scope(local_ctx);

    const int argc = 1;
    auto store = v8::ArrayBuffer::NewBackingStore(
      data.get_write(), data.size(), v8::BackingStore::EmptyDeleter, nullptr);
    auto data_array_buf = v8::ArrayBuffer::New(_isolate, std::move(store));
    v8::Local<v8::Value> argv[argc] = {data_array_buf};
    v8::Local<v8::Value> result;

    v8::Local<v8::Function> local_function = v8::Local<v8::Function>::New(
      _isolate, _function);
    if (!local_function->Call(local_ctx, local_ctx->Global(), argc, argv)
           .ToLocal(&result)) {
        throw_exception_from_v8(try_catch, "Can not run function");
    }

    return ss::now();
}

void instance::throw_exception_from_v8(
  const v8::TryCatch& try_catch, std::string_view msg) {
    v8::String::Utf8Value error(_isolate, try_catch.Exception());
    throw instance_exception(
      fmt::format("{}:{}", msg, std::string(*error, error.length())));
}

} // namespace v8_engine
