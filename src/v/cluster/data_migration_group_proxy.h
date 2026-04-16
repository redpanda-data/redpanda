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

#include "base/outcome.h"
#include "cluster/offsets_snapshot.h"
#include "container/chunked_vector.h"
#include "kafka/protocol/types.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>

#include <memory>

namespace cluster::data_migrations {

/*
 * Abstract class wrapper for a proxy component to operate kafka layer consumer
 * groups from data migration components.
 */
class group_proxy {
public:
    class impl {
    public:
        virtual ~impl() = default;

        virtual std::optional<model::partition_id>
        partition_for(const kafka::group_id& group) = 0;

        virtual ss::future<result<model::offset>> set_blocked_for_groups(
          const model::ntp& co_ntp,
          const chunked_vector<kafka::group_id>&,
          bool to_block,
          model::revision_id revision_id) = 0;

        virtual ss::future<std::error_code> delete_groups(
          const model::ntp& co_ntp,
          const chunked_vector<kafka::group_id>& groups,
          model::revision_id revision_id) = 0;

        virtual ss::future<bool>
        assure_topic_exists(model::timeout_clock::time_point deadline) = 0;

        virtual ss::future<get_group_offsets_reply>
        get_group_offsets(get_group_offsets_request&& req) = 0;

        virtual ss::future<set_group_offsets_reply>
        set_group_offsets(set_group_offsets_request&& req) = 0;
    };

    explicit group_proxy(std::unique_ptr<impl> impl)
      : _impl(std::move(impl)) {}

    ss::future<> stop() { return _gate.close(); }

    std::optional<model::partition_id>
    partition_for(const kafka::group_id& group) {
        if (_gate.is_closed()) {
            return std::nullopt;
        }
        return _impl->partition_for(group);
    }

    ss::future<result<model::offset>> set_blocked_for_groups(
      const model::ntp& co_ntp,
      const chunked_vector<kafka::group_id>& groups,
      bool to_block,
      model::revision_id revision_id) {
        auto holder = _gate.hold();
        co_return co_await _impl->set_blocked_for_groups(

          co_ntp, groups, to_block, revision_id);
    }

    ss::future<std::error_code> delete_groups(
      const model::ntp& co_ntp,
      const chunked_vector<kafka::group_id>& groups,
      model::revision_id revision_id) {
        auto holder = _gate.hold();
        co_return co_await _impl->delete_groups(co_ntp, groups, revision_id);
    }

    ss::future<bool>
    assure_topic_exists(model::timeout_clock::time_point deadline) {
        auto holder = _gate.hold();
        co_return co_await _impl->assure_topic_exists(deadline);
    }

    ss::future<get_group_offsets_reply>
    get_group_offsets(get_group_offsets_request&& req) {
        auto holder = _gate.hold();
        co_return co_await _impl->get_group_offsets(std::move(req));
    }

    ss::future<set_group_offsets_reply>
    set_group_offsets(set_group_offsets_request&& req) {
        auto holder = _gate.hold();
        co_return co_await _impl->set_group_offsets(std::move(req));
    }

private:
    std::unique_ptr<impl> _impl;
    ss::gate _gate;
};

} // namespace cluster::data_migrations
