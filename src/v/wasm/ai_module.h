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
#include "wasm/ffi.h"

#include <cstdint>

namespace wasm {

/**
 * The AI Module for Redpanda locally running models.
 */
class ai_module {
public:
    explicit ai_module(ai::service* service);
    static constexpr std::string_view name = "redpanda_ai";

    ss::future<int32_t> generate_text(
      ss::sstring prompt,
      int32_t max_tokens,
      ffi::array<uint8_t> generated_output);

private:
    ai::service* _service;
};

} // namespace wasm
