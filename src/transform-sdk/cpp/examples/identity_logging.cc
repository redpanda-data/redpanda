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

#include <print>

int main() {
    // This is an example of copying records from one location to another, with
    // some printing.
    redpanda::on_record_written(
      [](redpanda::write_event event, redpanda::record_writer* writer) {
          redpanda::bytes_view raw_key = event.record.key.value_or(
            redpanda::bytes_view{});
          redpanda::bytes_view raw_value = event.record.value.value_or(
            redpanda::bytes_view{});
          auto key = std::string_view{raw_key};
          auto value = std::string_view{raw_value};
          std::println(stderr, "{}:{}", key, value);
          return writer->write(event.record);
      });
}
