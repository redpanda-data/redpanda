/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "coproc/types.h"
#include "model/adl_serde.h"
#include "reflection/async_adl.h"

namespace reflection {

template<>
struct async_adl<coproc::process_batch_request> {
    ss::future<> to(iobuf& out, coproc::process_batch_request&&);
    ss::future<coproc::process_batch_request> from(iobuf_parser&);
};

template<>
struct async_adl<coproc::process_batch_request::data> {
    ss::future<> to(iobuf& out, coproc::process_batch_request::data&&);
    ss::future<coproc::process_batch_request::data> from(iobuf_parser&);
};

template<>
struct async_adl<coproc::process_batch_reply> {
    ss::future<> to(iobuf& out, coproc::process_batch_reply&&);
    ss::future<coproc::process_batch_reply> from(iobuf_parser&);
};

template<>
struct async_adl<coproc::process_batch_reply::data> {
    ss::future<> to(iobuf& out, coproc::process_batch_reply::data&&);
    ss::future<coproc::process_batch_reply::data> from(iobuf_parser&);
};

} // namespace reflection
