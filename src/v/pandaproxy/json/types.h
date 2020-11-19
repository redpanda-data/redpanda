/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include <cstdint>

namespace pandaproxy::json {

enum class serialization_format : uint8_t { none = 0, binary_v2, unsupported };

template<typename T>
class rjson_parse_impl;

} // namespace pandaproxy::json
