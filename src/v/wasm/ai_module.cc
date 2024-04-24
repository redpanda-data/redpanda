/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "ai_module.h"

#include "ai/service.h"

#include <boost/algorithm/string/predicate.hpp>

#include <utility>

namespace wasm {
ss::future<model_handle>
ai_module::load_hf_embeddings(ss::sstring model_repo, ss::sstring model_file) {
    ai::huggingface_file hff{
      .repo = std::move(model_repo), .filename = std::move(model_file)};
    auto models = co_await _service->list_models();
    for (const auto& m : models) {
        if (m.file == hff) {
            co_return allocate_handle(m.id);
        }
    }
    auto id = co_await _service->deploy_embeddings_model(hff);
    co_return allocate_handle(id);
}

ss::future<model_handle>
ai_module::load_hf_llm(ss::sstring model_repo, ss::sstring model_file) {
    ai::huggingface_file hff{
      .repo = std::move(model_repo), .filename = std::move(model_file)};
    auto models = co_await _service->list_models();
    for (const auto& m : models) {
        if (m.file == hff) {
            co_return allocate_handle(m.id);
        }
    }
    auto id = co_await _service->deploy_text_generation_model(hff);
    co_return allocate_handle(id);
}

ss::future<int32_t> ai_module::generate_text(
  model_handle handle,
  ss::sstring prompt,
  int32_t max_tokens,
  ffi::array<uint8_t> generated_output) {
    auto id = translate(handle);
    if (!id) {
        co_return -1;
    }
    auto result = co_await _service->generate_text(
      *id, std::move(prompt), {.max_tokens = max_tokens});

    size_t copy_n = std::min(generated_output.size(), result.size());
    for (size_t i = 0; i < copy_n; ++i) {
        generated_output[i] = uint8_t(result[i]);
    }
    co_return int32_t(result.size());
}

ss::future<int32_t> ai_module::compute_embeddings(
  model_handle handle, ss::sstring text, ffi::array<float> generated_output) {
    auto id = translate(handle);
    if (!id) {
        co_return -1;
    }
    auto result = co_await _service->compute_embeddings(*id, std::move(text));

    size_t copy_n = std::min(generated_output.size(), result.size());
    for (size_t i = 0; i < copy_n; ++i) {
        generated_output[i] = result[i];
    }
    co_return int32_t(result.size());
}

ai_module::ai_module(ai::service* service)
  : _service(service) {}

std::optional<ai::model_id> ai_module::translate(model_handle handle) const {
    auto it = _mapping.find(handle);
    if (it == _mapping.end()) {
        return std::nullopt;
    }
    return it->second;
}
model_handle ai_module::allocate_handle(ai::model_id id) {
    for (const auto& [h, existing] : _mapping) {
        if (existing == id) {
            return h;
        }
    }
    // Otherwise new handle.
    model_handle h{++_next_handle};
    _mapping[h] = id;
    return h;
}
} // namespace wasm
