/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "serde/protobuf/base.h"

namespace serde::pb {

std::optional<field>
base_message::lookup_field_by_path(std::span<std::string_view> field_path) {
    return convert_field_path_to_numbers(field_path)
      .and_then([this](std::vector<int32_t> field_numbers) {
          return lookup_field(field_numbers);
      });
}

} // namespace serde::pb
