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

#include "pandaproxy/rest/proxy.h"
#include "pandaproxy/server.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

namespace pandaproxy::rest {

ss::future<proxy::server::reply_t>
get_brokers(proxy::server::request_t rq, proxy::server::reply_t rp);

ss::future<proxy::server::reply_t>
get_topics_names(proxy::server::request_t rq, proxy::server::reply_t rp);

ss::future<proxy::server::reply_t>
get_topics_records(proxy::server::request_t rq, proxy::server::reply_t rp);

ss::future<proxy::server::reply_t>
post_topics_name(proxy::server::request_t rq, proxy::server::reply_t rp);

ss::future<proxy::server::reply_t>
create_consumer(proxy::server::request_t rq, proxy::server::reply_t rp);

ss::future<proxy::server::reply_t>
remove_consumer(proxy::server::request_t rq, proxy::server::reply_t rp);

ss::future<proxy::server::reply_t>
subscribe_consumer(proxy::server::request_t rq, proxy::server::reply_t rp);

ss::future<proxy::server::reply_t>
consumer_fetch(proxy::server::request_t rq, proxy::server::reply_t rp);

ss::future<proxy::server::reply_t>
get_consumer_offsets(proxy::server::request_t rq, proxy::server::reply_t rp);

ss::future<proxy::server::reply_t>
post_consumer_offsets(proxy::server::request_t rq, proxy::server::reply_t rp);

ss::future<proxy::server::reply_t>
status_ready(proxy::server::request_t rq, proxy::server::reply_t rp);

} // namespace pandaproxy::rest
