// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <redpanda/transform_sdk.h>

#include <array>
#include <cstdint>

#ifdef __wasi__
#define WASM_IMPORT(mod, name)                                                 \
    __attribute__((import_module(#mod), import_name(#name)))
#else
#define WASM_IMPORT(mod, name)
#endif

extern "C" {
WASM_IMPORT(redpanda_ai, generate_text)
int32_t redpanda_ai_generate_text(
  const uint8_t* prompt_data,
  size_t prompt_len,
  size_t max_tokens,
  const uint8_t* generated_output,
  size_t generated_output_len);
}

namespace rp {
using namespace redpanda;
} // namespace rp

std::error_code
generate_transform(const rp::write_event& event, rp::record_writer* writer) {
    rp::bytes_view value = event.record.value.value_or(rp::bytes_view());
    static constexpr size_t max_output_size = 2048;
    static constexpr size_t max_tokens = 100;
    std::array<uint8_t, max_output_size> output;
    size_t generated_amount = redpanda_ai_generate_text(
      value.data(), value.size(), max_tokens, output.data(), output.size());
    return writer->write({
      .key = event.record.key,
      .value = std::make_optional<rp::bytes_view>(
        output.data(), generated_amount),
      .headers = event.record.headers,
    });
}

int main() { rp::on_record_written(generate_transform); }
