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

#include "bytes/iobuf.h"
#include "kafka/types.h"

#include <optional>

namespace kafka {

///\brief batch_reader owns an iobuf that's a concatenation of
/// model::record_batch on the wire
///
/// The array bound is not serialized, c.f. array<kafka::thing>
class batch_reader {
public:
    batch_reader() = default;

    explicit batch_reader(iobuf buf)
      : _buf(std::move(buf)) {}

    bool empty() const { return _buf.empty(); }

    size_t size_bytes() const { return _buf.size_bytes(); }

    // Release any remaining iobuf that hasn't been consumed
    iobuf release() && { return std::move(_buf); }

private:
    iobuf _buf;
};

} // namespace kafka
