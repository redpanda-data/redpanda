/*
 * Copyright 2026 Redpanda Data, Inc.
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
#include "pandaproxy/rest/proxy.h"

#include <seastar/core/future.hh>

namespace pandaproxy::rest {
ss::future<proxy::server::reply_t>
get_translation_state(proxy::server::request_t rq, proxy::server::reply_t rp);

}
