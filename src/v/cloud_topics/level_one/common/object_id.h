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
#include "utils/named_type.h"
#include "utils/uuid.h"

namespace experimental::cloud_topics::l1 {

// An object ID is a unique identifier for a cloud topic L1 object.
using object_id = named_type<uuid_t, struct l1_object_id_tag>;

inline object_id create_object_id() { return object_id{uuid_t::create()}; }

// An extent of a remote object, which is a pair of offset and size.
struct object_extent {
    object_id id;
    size_t position = 0;
    size_t size = 0;

    fmt::iterator format_to(fmt::iterator it) const;
};

} // namespace experimental::cloud_topics::l1
