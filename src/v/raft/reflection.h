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

#include "model/adl_serde.h"
#include "raft/types.h"
#include "reflection/async_adl.h"

namespace reflection {

template<>
struct async_adl<raft::append_entries_request> {
    ss::future<> to(iobuf& out, raft::append_entries_request&& request);
    ss::future<raft::append_entries_request> from(iobuf_parser& in);
};
template<>
struct adl<raft::protocol_metadata> {
    void to(iobuf& out, raft::protocol_metadata request);
    raft::protocol_metadata from(iobuf_parser& in);
};
template<>
struct async_adl<raft::heartbeat_request> {
    ss::future<> to(iobuf& out, raft::heartbeat_request&& request);
    ss::future<raft::heartbeat_request> from(iobuf_parser& in);
};

template<>
struct async_adl<raft::heartbeat_reply> {
    ss::future<> to(iobuf& out, raft::heartbeat_reply&& request);
    ss::future<raft::heartbeat_reply> from(iobuf_parser& in);
};

template<>
struct adl<raft::snapshot_metadata> {
    void to(iobuf& out, raft::snapshot_metadata&& request);
    raft::snapshot_metadata from(iobuf_parser& in);
};

template<>
struct adl<raft::vnode> {
    void to(iobuf&, raft::vnode);
    raft::vnode from(iobuf_parser&);
};
template<>
struct adl<raft::group_configuration> {
    void to(iobuf&, raft::group_configuration);
    raft::group_configuration from(iobuf_parser&);
};

} // namespace reflection
