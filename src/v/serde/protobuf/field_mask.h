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

#pragma once

#include "base/format_to.h"
#include "base/seastarx.h"
#include "container/chunked_vector.h"

#include <seastar/core/sstring.hh>

namespace serde::pb {

/**
 * A field mask is a protobuf message that specifies which fields
 * should be considered set in an other protobuf message.
 *
 * It is often used for partial updates, where only a subset of fields
 * are modified.
 *
 * See: https://protobuf.dev/reference/protobuf/google.protobuf/#field-mask
 * for more details.
 */
struct field_mask {
    chunked_vector<ss::sstring> paths;

    bool operator==(const field_mask& other) const = default;
    fmt::iterator format_to(fmt::iterator it) const;
};

} // namespace serde::pb
