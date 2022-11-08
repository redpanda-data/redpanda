/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "config/rest_authn_endpoint.h"
#include "pandaproxy/types.h"
#include "utils/mutex.h"

#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/timer.hh>

#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/multi_index_container.hpp>

#include <chrono>
#include <functional>
#include <list>

namespace pandaproxy {

// A LRU cache implemented with a doubly-linked list and a
// hash using boost multi container. The list tracks
// frequency where the most recently used client is at the front.
// When the cache is full remove the client from the end of the
// list. The hash is for constant time look-ups of the kafka clients.
class kafka_client_cache {
public:
    kafka_client_cache(
      YAML::Node const& cfg,
      size_t max_size,
      std::chrono::milliseconds keep_alive);

    ~kafka_client_cache() = default;

    kafka_client_cache(kafka_client_cache&&) = default;
    kafka_client_cache(kafka_client_cache const&) = delete;
    kafka_client_cache& operator=(kafka_client_cache&&) = delete;
    kafka_client_cache& operator=(kafka_client_cache const&) = delete;

    ss::future<> start();
    ss::future<> stop();

    client_ptr
    make_client(credential_t user, config::rest_authn_method authn_method);

    template<
      std::invocable<kafka::client::client&> Func,
      typename Futurator
      = ss::futurize<std::invoke_result_t<Func, kafka::client::client&>>>
    typename Futurator::type with_client_for(
      credential_t user, config::rest_authn_method authn_method, Func&& func) {
        auto [client, client_mu] = fetch_or_insert(
          std::move(user), authn_method);
        auto units = co_await client_mu->get_units();
        co_return co_await Futurator::invoke(std::forward<Func>(func), *client);
    }

    ss::future<> clean_stale_clients();

    size_t size() const;
    size_t max_size() const;

protected:
    std::pair<client_ptr, client_mu_ptr>
    fetch_or_insert(credential_t user, config::rest_authn_method authn_method);

private:
    // Tags used for indexing
    struct underlying_list {};
    struct underlying_hash {};

    // Conceptually there is a single container underneath multi_index
    // and we can query the container by a list (i.e., sequence) or
    // hash interface.
    using underlying_t = boost::multi_index::multi_index_container<
      timestamped_user,
      boost::multi_index::indexed_by<
        boost::multi_index::sequenced<boost::multi_index::tag<underlying_list>>,
        boost::multi_index::hashed_unique<
          boost::multi_index::tag<underlying_hash>,
          boost::multi_index::
            member<timestamped_user, ss::sstring, &timestamped_user::key>,
          std::hash<ss::sstring>,
          std::equal_to<>>>>;

    kafka::client::configuration _config;
    size_t _cache_max_size;
    std::chrono::milliseconds _keep_alive;
    underlying_t _cache;
    std::list<timestamped_user> _evicted_items;
    ss::timer<ss::lowres_clock> _gc_timer;
    ss::gate _gc_gate;
    mutex _gc_lock;
};
} // namespace pandaproxy
