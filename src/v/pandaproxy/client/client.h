#pragma once

#include "kafka/client.h"
#include "pandaproxy/client/broker.h"
#include "pandaproxy/client/brokers.h"
#include "pandaproxy/client/configuration.h"
#include "utils/retry.h"
#include "utils/unresolved_address.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/semaphore.hh>

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

namespace pandaproxy::client {

/// \brief wait or start a function
///
/// Start the function and wait for it to finish, or, if an instance of the
/// function is already running, wait for that one to finish.
class wait_or_start {
public:
    // Prevent accidentally calling the protected func.
    struct tag {};
    using func = ss::noncopyable_function<ss::future<>(tag)>;

    explicit wait_or_start(func func)
      : _func{std::move(func)} {}

    ss::future<> operator()() {
        if (_lock.try_wait()) {
            return _func(tag{}).finally(
              [this]() { _lock.signal(_lock.waiters() + 1); });
        }
        return _lock.wait();
    }

private:
    func _func;
    ss::semaphore _lock{1};
};

class client {
public:
    explicit client(std::vector<unresolved_address> broker_addrs);

    /// \brief Connect to all brokers.
    ss::future<> connect();
    /// \brief Disconnect from all brokers.
    ss::future<> stop();

    /// \brief Dispatch a request to any broker.
    template<typename T>
    CONCEPT(requires(KafkaRequest<typename T::api_type>))
    ss::future<typename T::api_type::response_type> dispatch(T r) {
        return _brokers.any().then(
          [r{std::move(r)}](shared_broker_t broker) mutable {
              return broker->dispatch(std::move(r));
          });
    }

private:
    /// \brief Connect and update metdata.
    ss::future<> do_connect(unresolved_address addr);

    /// \brief Update metadata
    ///
    /// If an existing update is in progress, the future returned will be
    /// satisfied by the outstanding request.
    ///
    /// Uses round-robin load-balancing strategy.
    ss::future<> update_metadata(wait_or_start::tag);

    /// \brief Seeds are used when no brokers are connected.
    std::vector<unresolved_address> _seeds;
    /// \brief Broker lookup from topic_partition.
    brokers _brokers;
    /// \brief Update metadata, or wait for an existing one.
    wait_or_start _wait_or_start_update_metadata;
};

} // namespace pandaproxy::client
