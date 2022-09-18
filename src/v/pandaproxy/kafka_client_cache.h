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
#include "config/broker_authn_endpoint.h"
#include "config/rest_authn_endpoint.h"
#include "pandaproxy/types.h"

#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/multi_index_container.hpp>

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/timer.hh>

#include <utility>
#include <vector>
#include <chrono>

using namespace std::chrono_literals;

namespace bmi = boost::multi_index;

namespace pandaproxy {

class sharded_client_cache;

// A LRU cache implemented with a doubly-linked list and a
// hash using boost multi container. The list tracks
// frequency where the most recently used client is at the front.
// When the cache is full remove the client from the end of the
// list. The hash is for constant time look-ups of the kafka clients.
class kafka_client_cache {
public:
    struct user_client_pair {
        ss::sstring username;
        client_ptr client;
    };

    // Tags used for indexing
    struct underlying_list {};
    struct underlying_hash {};

    using underlying_t = bmi::multi_index_container<
      user_client_pair,
      bmi::indexed_by<
        bmi::sequenced<bmi::tag<underlying_list>>,
        bmi::hashed_unique<
          bmi::tag<underlying_hash>,
          bmi::
            member<user_client_pair, ss::sstring, &user_client_pair::username>,
          std::hash<ss::sstring>,
          std::equal_to<>>>>;

    kafka_client_cache(
      YAML::Node const& cfg,
      size_t max_size,
      std::vector<config::broker_authn_endpoint> kafka_api,
      model::timestamp::type keep_alive);

    ~kafka_client_cache();

    kafka_client_cache(const kafka_client_cache&) = delete;
    kafka_client_cache& operator=(const kafka_client_cache&) = delete;

    client_ptr fetch_or_insert(credential_t user, config::rest_authn_type authn_type);

    size_t size() const;
    size_t max_size() const;
    static std::chrono::milliseconds clean_timer();

private:
    friend class test_client_cache;
    client_ptr fetch(credential_t user);
    void insert(credential_t user, client_ptr client);
    client_ptr
    make_client(credential_t user, config::rest_authn_type authn_type);
    void clean_stale_clients();

    kafka::client::configuration _config;
    size_t _cache_max_size;
    bool _kafka_has_sasl;
    model::timestamp::type _keep_alive;
    underlying_t _cache;
    ss::timer<ss::lowres_clock> _clean_timer;
};
} // namespace pandaproxy
