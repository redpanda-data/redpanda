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
#include "pandaproxy/logger.h"
#include "pandaproxy/types.h"
#include "security/credential_store.h"
#include "ssx/future-util.h"

#include <seastar/core/loop.hh>

#include <absl/container/node_hash_map.h>
#include <fmt/format.h>

#include <list>
#include <stdexcept>
#include <utility>
#include <vector>

namespace pandaproxy {

class sharded_client_cache;

// A LRU cache implemented with a doubly-linked list and an
// unordered map. The list tracks frequency where the most
// recently used client is at the front. When the cache is full
// remove the client from the end of the list. The map is for
// constant time look-ups of the kafka clients.
class kafka_client_cache {
    friend class sharded_client_cache;

public:
    using user_client_pair = std::pair<ss::sstring, client_ptr>;
    using user_client_list_it = std::list<user_client_pair>::iterator;
    using user_client_map
      = absl::flat_hash_map<ss::sstring, user_client_list_it>;

    kafka_client_cache(
      YAML::Node const& cfg,
      size_t size,
      std::vector<config::broker_authn_endpoint> kafka_api,
      model::timestamp::type keep_alive = 30000);

    kafka_client_cache(const kafka_client_cache&) = delete;
    kafka_client_cache& operator=(const kafka_client_cache&) = delete;

    client_ptr fetch(credential_t user);
    void insert(credential_t user, client_ptr client);

    size_t size() const;

private:
    client_ptr
    make_client(credential_t user, config::rest_authn_type authn_type);
    void clean_stale_clients();

    kafka::client::configuration _config;
    size_t _cache_size;
    bool _kafka_has_sasl;
    model::timestamp::type _keep_alive;
    std::list<user_client_pair> _user_client_list;
    user_client_map _user_client_map;
};
} // namespace pandaproxy
