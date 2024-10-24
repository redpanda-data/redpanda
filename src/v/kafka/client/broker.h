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

#include "kafka/client/configuration.h"
#include "kafka/client/exceptions.h"
#include "kafka/client/logger.h"
#include "kafka/client/transport.h"
#include "model/metadata.h"
#include "net/connection.h"
#include "utils/mutex.h"

#include <seastar/core/gate.hh>
#include <seastar/core/shared_ptr.hh>

#include <absl/container/flat_hash_set.h>

namespace kafka::client {

struct gated_mutex {
    gated_mutex()
      : _mutex{"gated_mutex"}
      , _gate{} {}

    template<typename Func>
    auto with(Func&& func) noexcept {
        return ss::try_with_gate(
          _gate, [this, func{std::forward<Func>(func)}]() mutable {
              return _mutex.with(
                [this, func{std::forward<Func>(func)}]() mutable {
                    _gate.check();
                    return func();
                });
          });
    }

    ss::future<> close() { return _gate.close(); }

    mutex _mutex{"gated_mutex"};
    ss::gate _gate;
};

class broker : public ss::enable_lw_shared_from_this<broker> {
public:
    broker(model::node_id node_id, transport&& client)
      : _node_id(node_id)
      , _client(std::move(client))
      , _gated_mutex{} {}

    template<typename T, typename Ret = typename T::api_type::response_type>
    requires(KafkaApi<typename T::api_type>)
    ss::future<Ret> dispatch(T r) {
        using api_t = typename T::api_type;
        return _gated_mutex
          .with([this, r{std::move(r)}]() mutable {
              vlog(
                kcwire.debug,
                "{}Dispatch to node {}: {} req: {}",
                *this,
                _node_id,
                api_t::name,
                r);
              return _client.dispatch(std::move(r)).then([this](Ret res) {
                  vlog(
                    kcwire.debug,
                    "{}Dispatch from node {}: {} res: {}",
                    *this,
                    _node_id,
                    api_t::name,
                    res);
                  return res;
              });
          })
          .handle_exception_type(
            [this](const kafka_request_disconnected_exception&) {
                // Short read
                return ss::make_exception_future<Ret>(
                  broker_error(_node_id, error_code::broker_not_available));
            })
          .handle_exception_type([this](const std::system_error& e) {
              if (net::is_reconnect_error(e)) {
                  return ss::make_exception_future<Ret>(
                    broker_error(_node_id, error_code::broker_not_available));
              }
              return ss::make_exception_future<Ret>(e);
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
    /// \brief Log the client ID if it exists, otherwise don't log
    friend std::ostream& operator<<(std::ostream& os, const broker& b) {
        if (b._client.client_id().has_value()) {
            fmt::print(os, "{}: ", b._client.client_id().value());
        }
        return os;
    }

    model::node_id _node_id;
    transport _client;
    // TODO(Ben): allow overlapped requests
    gated_mutex _gated_mutex;
};

using shared_broker_t = ss::lw_shared_ptr<broker>;

ss::future<shared_broker_t> make_broker(
  model::node_id node_id,
  net::unresolved_address addr,
  const configuration& config);

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

} // namespace kafka::client
