/*
 * Copyright 2025 Redpanda Data, Inc.
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
#include "kinds.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sstring.hh>

#include <sys/types.h>

namespace cluster::sloth_mail {
// for easier duplicate detection must be in the order of kind_id values
class config {
public:
    explicit config(int) {};

    using supported_kinds = supported_kinds;

    ss::future<impl::mail_reply>
    do_ship_mail(model::node_id, impl::types<supported_kinds>::mail_request&&) {
        // TODO: implement using RPC
        co_return impl::mail_reply{.ec = errc::success};
    };
};
} // namespace cluster::sloth_mail
