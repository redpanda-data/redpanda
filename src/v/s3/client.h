/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "http/client.h"
#include "rpc/transport.h"
#include "rpc/types.h"
#include "s3/client_probe.h"
#include "s3/signature.h"
#include "utils/gate_guard.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/lowres_clock.hh>

#include <boost/iterator/counting_iterator.hpp>
#include <boost/property_tree/ptree_fwd.hpp>
#include <boost/range/counting_range.hpp>

#include <chrono>
#include <initializer_list>
#include <limits>

namespace s3 {

using access_point_uri = named_type<ss::sstring, struct s3_access_point_uri>;
using bucket_name = named_type<ss::sstring, struct s3_bucket_name>;
using object_key = named_type<std::filesystem::path, struct s3_object_key>;
using endpoint_url = named_type<ss::sstring, struct s3_endpoint_url>;
using ca_trust_file
  = named_type<std::filesystem::path, struct s3_ca_trust_file>;

struct object_tag {
    ss::sstring key;
    ss::sstring value;
};

/// List of default overrides that can be used to workaround issues
/// that can arise when we want to deal with different S3 API implementations
/// and different OS issues (like different truststore locations on different
/// Linux distributions).
struct default_overrides {
    std::optional<endpoint_url> endpoint = std::nullopt;
    std::optional<uint16_t> port = std::nullopt;
    std::optional<ca_trust_file> trust_file = std::nullopt;
    bool disable_tls = false;
};

/// S3 client configuration
struct configuration : rpc::base_transport::configuration {
    /// URI of the S3 access point
    access_point_uri uri;
    /// AWS access key
    public_key_str access_key;
    /// AWS secret key
    private_key_str secret_key;
    /// AWS region
    aws_region_name region;
    /// Metrics probe (should be created for every aws account on every shard)
    ss::shared_ptr<client_probe> _probe;

    /// \brief opinionated configuraiton initialization
    /// Generates uri field from region, initializes credentials for the
    /// transport, resolves the uri to get the server_addr.
    ///
    /// \param pkey is an AWS access key
    /// \param skey is an AWS secret key
    /// \param region is an AWS region code
    /// \param overrides contains a bunch of property overrides like
    ///        non-standard SSL port and alternative location of the
    ///        truststore
    /// \return future that returns initialized configuration
    static ss::future<configuration> make_configuration(
      const public_key_str& pkey,
      const private_key_str& skey,
      const aws_region_name& region,
      const default_overrides& overrides = {},
      rpc::metrics_disabled disable_metrics = rpc::metrics_disabled::yes);
};

std::ostream& operator<<(std::ostream& o, const configuration& c);

/// Request formatter for AWS S3
class request_creator {
public:
    /// C-tor
    /// \param conf is a configuration container
    explicit request_creator(const configuration& conf);

    /// \brief Create unsigned 'PutObject' request header
    /// The payload is unsigned which means that we don't need to calculate
    /// hash from it (which don't want to do for large files).
    ///
    /// \param name is a bucket that should be used to store new object
    /// \param key is an object name
    /// \param payload_size_bytes is a size of the object in bytes
    /// \return initialized and signed http header or error
    result<http::client::request_header> make_unsigned_put_object_request(
      bucket_name const& name,
      object_key const& key,
      size_t payload_size_bytes,
      const std::vector<object_tag>& tags);

    /// \brief Create a 'GetObject' request header
    ///
    /// \param name is a bucket that has the object
    /// \param key is an object name
    /// \return initialized and signed http header or error
    result<http::client::request_header>
    make_get_object_request(bucket_name const& name, object_key const& key);

    /// \brief Create a 'DeleteObject' request header
    ///
    /// \param name is a bucket that has the object
    /// \param key is an object name
    /// \return initialized and signed http header or error
    result<http::client::request_header>
    make_delete_object_request(bucket_name const& name, object_key const& key);

    /// \brief Initialize http header for 'ListObjectsV2' request
    ///
    /// \param name of the bucket
    /// \param region to connect
    /// \param max_keys is a max number of returned objects
    /// \param offset is an offset of the first returned object
    /// \return initialized and signed http header or error
    result<http::client::request_header> make_list_objects_v2_request(
      const bucket_name& name,
      std::optional<object_key> prefix,
      std::optional<object_key> start_after,
      std::optional<size_t> max_keys);

private:
    access_point_uri _ap;
    signature_v4 _sign;
};

/// S3 REST-API client
class client {
public:
    explicit client(const configuration& conf);
    client(const configuration& conf, const ss::abort_source& as);

    /// Stop the client
    ss::future<> stop();
    /// Shutdown the underlying connection
    ss::future<> shutdown();

    /// Download object from S3 bucket
    ///
    /// \param name is a bucket name
    /// \param key is an object key
    /// \return future that gets ready after request was sent
    ss::future<http::client::response_stream_ref> get_object(
      bucket_name const& name,
      object_key const& key,
      const ss::lowres_clock::duration& timeout);

    /// Put object to S3 bucket.
    /// \param name is a bucket name
    /// \param key is an id of the object
    /// \param payload_size is a size of the object in bytes
    /// \param body is an input_stream that can be used to read body
    /// \return future that becomes ready when the upload is completed
    ss::future<> put_object(
      bucket_name const& name,
      object_key const& key,
      size_t payload_size,
      ss::input_stream<char>&& body,
      const std::vector<object_tag>& tags,
      const ss::lowres_clock::duration& timeout);

    struct list_bucket_item {
        ss::sstring key;
        std::chrono::system_clock::time_point last_modified;
        size_t size_bytes;
        ss::sstring etag;
    };
    struct list_bucket_result {
        bool is_truncated;
        ss::sstring prefix;
        std::vector<list_bucket_item> contents;
    };
    ss::future<list_bucket_result> list_objects_v2(
      const bucket_name& name,
      std::optional<object_key> prefix = std::nullopt,
      std::optional<object_key> start_after = std::nullopt,
      std::optional<size_t> max_keys = std::nullopt,
      const ss::lowres_clock::duration& timeout
      = http::default_connect_timeout);

    ss::future<> delete_object(
      const bucket_name& bucket,
      const object_key& key,
      const ss::lowres_clock::duration& timeout);

private:
    request_creator _requestor;
    http::client _client;
    ss::shared_ptr<client_probe> _probe;
};

/// Policy that controls behaviour of the client pool
/// in situation when number of requested client connections
/// exceeds pool capacity
enum class client_pool_overdraft_policy {
    /// Client pool should wait unitl any existing lease will be canceled
    wait_if_empty,
    /// Client pool should create transient client connection to serve the
    /// request
    create_new_if_empty
};

/// Connection pool implementation
/// All connections share the same configuration
class client_pool : public ss::weakly_referencable<client_pool> {
public:
    using http_client_ptr = ss::shared_ptr<client>;
    struct client_lease {
        http_client_ptr client;
        ss::deleter deleter;
    };

    client_pool(
      size_t size,
      configuration conf,
      client_pool_overdraft_policy policy
      = client_pool_overdraft_policy::wait_if_empty);

    ss::future<> stop();

    /// \brief Acquire http client from the pool.
    ///
    /// \note it's guaranteed that the client can only be acquired once
    ///       before it gets released (release happens implicitly, when
    ///       the lifetime of the pointer ends).
    /// \return client pointer (via future that can wait if all clients
    ///         are in use)
    ss::future<client_lease> acquire();

    /// \brief Get number of connections
    size_t size() const noexcept;

private:
    void init();
    void release(ss::shared_ptr<client> leased);

    const size_t _size;
    configuration _config;
    client_pool_overdraft_policy _policy;
    std::vector<http_client_ptr> _pool;
    ss::condition_variable _cvar;
    ss::abort_source _as;
    ss::gate _gate;
};

} // namespace s3
