/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "ai/service.h"
#include "utils/named_type.h"
#include "wasm/ffi.h"

#include <absl/container/flat_hash_map.h>

#include <cmath>
#include <cstdint>

namespace wasm {

using model_handle = named_type<int32_t, struct model_handle_t>;

/**
 * The AI Module for Redpanda locally running models.
 */
class ai_module {
public:
    explicit ai_module(ai::service* service);
    static constexpr std::string_view name = "redpanda_ai";

    ss::future<model_handle>
    load_hf_embeddings(ss::sstring model_repo, ss::sstring model_file);
    ss::future<model_handle>
    load_hf_llm(ss::sstring model_repo, ss::sstring model_file);

    ss::future<int32_t> generate_text(
      model_handle,
      ss::sstring prompt,
      int32_t max_tokens,
      ffi::array<uint8_t> generated_output);

    ss::future<int32_t> compute_embeddings(
      model_handle, ss::sstring text, ffi::array<float> generated_output);

private:
    std::optional<ai::model_id> translate(model_handle) const;
    model_handle allocate_handle(ai::model_id id);

    int32_t _next_handle = 0;
    absl::flat_hash_map<model_handle, ai::model_id> _mapping;
    ai::service* _service;
};

} // namespace wasm
