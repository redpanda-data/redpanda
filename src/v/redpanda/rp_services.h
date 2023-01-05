/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "utils/string_switch.h"

#include <type_traits>

enum class service_kind {
    schema_registry,
};

constexpr std::string_view to_string_view(service_kind kind) {
    switch (kind) {
    case service_kind::schema_registry:
        return "schema-registry";
    }
    return "invalid";
}

template<typename E>
std::enable_if_t<std::is_enum_v<E>, std::optional<E>>
  from_string_view(std::string_view);

template<>
constexpr std::optional<service_kind>
from_string_view<service_kind>(std::string_view sv) {
    return string_switch<std::optional<service_kind>>(sv)
      .match(
        to_string_view(service_kind::schema_registry),
        service_kind::schema_registry)
      .default_match(std::nullopt);
}
