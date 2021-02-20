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

#include "pandaproxy/server.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

namespace pandaproxy {

ss::future<server::reply_t>
get_topics_names(server::request_t rq, server::reply_t rp);

ss::future<server::reply_t>
get_topics_records(server::request_t rq, server::reply_t rp);

ss::future<server::reply_t>
post_topics_name(server::request_t rq, server::reply_t rp);

ss::future<server::reply_t>
create_consumer(server::request_t rq, server::reply_t rp);

ss::future<server::reply_t>
subscribe_consumer(server::request_t rq, server::reply_t rp);

} // namespace pandaproxy
