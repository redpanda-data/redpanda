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

ss::future<ctx_server<proxy>::reply_t>
get_brokers(ctx_server<proxy>::request_t rq, ctx_server<proxy>::reply_t rp);

ss::future<ctx_server<proxy>::reply_t> get_topics_names(
  ctx_server<proxy>::request_t rq, ctx_server<proxy>::reply_t rp);

ss::future<ctx_server<proxy>::reply_t> get_topics_records(
  ctx_server<proxy>::request_t rq, ctx_server<proxy>::reply_t rp);

ss::future<ctx_server<proxy>::reply_t> post_topics_name(
  ctx_server<proxy>::request_t rq, ctx_server<proxy>::reply_t rp);

ss::future<ctx_server<proxy>::reply_t>
create_consumer(ctx_server<proxy>::request_t rq, ctx_server<proxy>::reply_t rp);

ss::future<ctx_server<proxy>::reply_t>
remove_consumer(ctx_server<proxy>::request_t rq, ctx_server<proxy>::reply_t rp);

ss::future<ctx_server<proxy>::reply_t> subscribe_consumer(
  ctx_server<proxy>::request_t rq, ctx_server<proxy>::reply_t rp);

ss::future<ctx_server<proxy>::reply_t>
consumer_fetch(ctx_server<proxy>::request_t rq, ctx_server<proxy>::reply_t rp);

ss::future<ctx_server<proxy>::reply_t> get_consumer_offsets(
  ctx_server<proxy>::request_t rq, ctx_server<proxy>::reply_t rp);

ss::future<ctx_server<proxy>::reply_t> post_consumer_offsets(
  ctx_server<proxy>::request_t rq, ctx_server<proxy>::reply_t rp);

} // namespace pandaproxy::rest
