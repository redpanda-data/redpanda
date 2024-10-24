// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/transform_utils.h"

#include "iceberg/time_transform_visitor.h"
#include "iceberg/transform.h"

namespace iceberg {

struct transform_applying_visitor {
    explicit transform_applying_visitor(const value& source_val)
      : source_val_(source_val) {}
    const value& source_val_;

    value operator()(const hour_transform&) {
        int_value v{std::visit(hour_transform_visitor{}, source_val_)};
        return v;
    }

    template<typename T>
    value operator()(const T&) {
        throw std::invalid_argument(
          "transform_applying_visitor not implemented for transform");
    }
};

value apply_transform(const value& source_val, const transform& transform) {
    return std::visit(transform_applying_visitor{source_val}, transform);
}

} // namespace iceberg
