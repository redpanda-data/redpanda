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
#include "cluster/sloth_mail/impl/types.h"
#include "cluster/sloth_mail/tests/kinds.h"
#include "container/chunked_vector.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sleep.hh>

#include <sys/types.h>

using namespace std::chrono_literals;

namespace cluster::sloth_mail::tests {

using mail_request = impl::types<supported_kinds>::mail_request;
struct node_mail_request {
    model::node_id destination;
    mail_request data;
};
using mail_requests = chunked_vector<node_mail_request>;

class mock_shipper {
public:
    ss::future<impl::mail_reply>
    do_ship_mail(model::node_id destination, mail_request&& r) {
        if (_injected_delay > 0ms) {
            // if no delay don't require a scheduling point in the test
            co_await ss::sleep(_injected_delay);
        }
        if (_cnt_failures_to_inject > 0) {
            --_cnt_failures_to_inject;
            co_return impl::mail_reply{.ec = errc::timeout};
        }
        _recent_requests.emplace_back(destination, std::move(r));
        co_return impl::mail_reply{.ec = errc::success};
    };

    mail_requests detach_recent_requests() {
        return std::move(_recent_requests);
    }

    void fail_next_n_requests(int n) { _cnt_failures_to_inject = n; }
    void inject_delay_for_all_requests(ss::lowres_clock::duration d) {
        _injected_delay = d;
    }

private:
    int _cnt_failures_to_inject = 0;
    ss::lowres_clock::duration _injected_delay = 0ms;
    mail_requests _recent_requests;
};

// for easier duplicate detection must be in the order of kind_id values
class config {
public:
    using supported_kinds = supported_kinds;

    config(mock_shipper& shipper)
      : _shipper(shipper) {}
    ss::future<impl::mail_reply>
    do_ship_mail(model::node_id destination, mail_request&& r) {
        return _shipper.do_ship_mail(destination, std::move(r));
    };

private:
    mock_shipper& _shipper;
};
} // namespace cluster::sloth_mail::tests
