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
#include "model/record_batch_reader.h"
#include "reflection/async_adl.h"

namespace reflection {

template<>
struct async_adl<model::record_batch_reader> {
    ss::future<> to(iobuf& out, model::record_batch_reader&&);

    ss::future<model::record_batch_reader> from(iobuf_parser&);
};

template<>
struct async_adl<model::record_batch> {
    ss::future<> to(iobuf& out, model::record_batch&&);
    ss::future<model::record_batch> from(iobuf_parser&);
};

} // namespace reflection
