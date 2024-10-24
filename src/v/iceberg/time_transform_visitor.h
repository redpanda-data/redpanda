// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "iceberg/values.h"

namespace iceberg {

struct hour_transform_visitor {
    int32_t operator()(const primitive_value& v);

    template<typename T>
    int32_t operator()(const T& t) {
        throw std::invalid_argument(
          fmt::format("hourly_visitor not implemented for value {}", t));
    }
};

} // namespace iceberg
