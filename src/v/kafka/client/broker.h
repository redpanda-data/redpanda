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

#include "kafka/client/transport.h"
#include "model/metadata.h"
#include "kafka/client/error.h"
#include "utils/mutex.h"

#include <seastar/core/gate.hh>
#include <seastar/core/shared_ptr.hh>

#include <absl/container/flat_hash_set.h>

namespace pandaproxy::client {

struct gated_mutex {
    gated_mutex()
      : _mutex{}
      , _gate{} {}

    template<typename Func>
    auto with(Func&& func) noexcept {
        return ss::with_gate(
          _gate, [this, func{std::forward<Func>(func)}]() mutable {
              return _mutex.with(
                [this, func{std::forward<Func>(func)}]() mutable {
                    _gate.check();
                    return func();
                });
          });
    }

    ss::future<> close() { return _gate.close(); }

    mutex _mutex;
    ss::gate _gate;
};

class broker : public ss::enable_lw_shared_from_this<broker> {
public:
    broker(model::node_id node_id, kafka::client::transport&& client)
      : _node_id(node_id)
      , _client(std::move(client))
      , _gated_mutex{} {}

    template<typename T, typename Ret = typename T::api_type::response_type>
    CONCEPT(requires(kafka::KafkaRequest<typename T::api_type>))
    ss::future<Ret> dispatch(T r) {
        return _gated_mutex
          .with([this, r{std::move(r)}]() mutable {
              return _client.dispatch(std::move(r));
          })
          .handle_exception_type([this](const std::bad_optional_access&) {
              // Short read
              return ss::make_exception_future<Ret>(broker_error(
                _node_id, kafka::error_code::broker_not_available));
          })
          .finally([b = shared_from_this()]() {});
    }

    model::node_id id() const { return _node_id; }
    ss::future<> stop() {
        return _gated_mutex.close()
          .then([this]() { return _client.stop(); })
          .finally([b = shared_from_this()]() {});
    }

private:
    model::node_id _node_id;
    kafka::client::transport _client;
    // TODO(Ben): allow overlapped requests
    gated_mutex _gated_mutex;
};

using shared_broker_t = ss::lw_shared_ptr<broker>;

ss::future<shared_broker_t>
make_broker(model::node_id node_id, unresolved_address addr);

struct broker_hash {
    using is_transparent = void;
    size_t operator()(const shared_broker_t& b) const {
        return absl::Hash<model::node_id>{}(b->id());
    }
    size_t operator()(model::node_id n_id) const {
        return absl::Hash<model::node_id>{}(n_id);
    }
};

struct broker_eq {
    using is_transparent = void;
    bool
    operator()(const shared_broker_t& lhs, const shared_broker_t& rhs) const {
        return lhs->id() == rhs->id();
    }
    bool operator()(model::node_id node_id, const shared_broker_t& b) const {
        return node_id == b->id();
    }
    bool operator()(const shared_broker_t& b, model::node_id node_id) const {
        return b->id() == node_id;
    }
};

} // namespace pandaproxy::client
