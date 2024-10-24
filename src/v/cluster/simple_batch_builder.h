/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "base/seastarx.h"
#include "reflection/adl.h"
#include "storage/record_batch_builder.h"

namespace cluster {

struct simple_batch_builder final : public storage::record_batch_builder {
    using storage::record_batch_builder::record_batch_builder;

    template<typename K, typename V>
    simple_batch_builder& add_kv(K key, V value) {
        add_raw_kv(
          reflection::to_iobuf(std::move(key)),
          reflection::to_iobuf(std::move(value)));
        return *this;
    }
};
} // namespace cluster
