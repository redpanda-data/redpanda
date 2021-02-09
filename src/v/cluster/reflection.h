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

#include "cluster/types.h"
#include "model/adl_serde.h"
#include "reflection/adl.h"

#include <fmt/ostream.h>

namespace reflection {

template<>
struct adl<model::timeout_clock::duration> {
    using rep = model::timeout_clock::rep;
    using duration = model::timeout_clock::duration;

    void to(iobuf& out, duration dur);
    duration from(iobuf_parser& in);
};

template<>
struct adl<cluster::topic_configuration> {
    void to(iobuf&, cluster::topic_configuration&&);
    cluster::topic_configuration from(iobuf_parser&);
};

template<>
struct adl<cluster::join_request> {
    void to(iobuf&, cluster::join_request&&);
    cluster::join_request from(iobuf);
    cluster::join_request from(iobuf_parser&);
};

template<>
struct adl<cluster::configuration_update_request> {
    void to(iobuf&, cluster::configuration_update_request&&);
    cluster::configuration_update_request from(iobuf_parser&);
};

template<>
struct adl<cluster::topic_result> {
    void to(iobuf&, cluster::topic_result&&);
    cluster::topic_result from(iobuf_parser&);
};

template<>
struct adl<cluster::create_topics_request> {
    void to(iobuf&, cluster::create_topics_request&&);
    cluster::create_topics_request from(iobuf);
    cluster::create_topics_request from(iobuf_parser&);
};

template<>
struct adl<cluster::create_topics_reply> {
    void to(iobuf&, cluster::create_topics_reply&&);
    cluster::create_topics_reply from(iobuf);
    cluster::create_topics_reply from(iobuf_parser&);
};
template<>
struct adl<cluster::topic_configuration_assignment> {
    void to(iobuf&, cluster::topic_configuration_assignment&&);
    cluster::topic_configuration_assignment from(iobuf_parser&);
};

template<>
struct adl<cluster::configuration_invariants> {
    void to(iobuf&, cluster::configuration_invariants&&);
    cluster::configuration_invariants from(iobuf_parser&);
};

} // namespace reflection
