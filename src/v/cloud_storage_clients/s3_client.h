/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/outcome.h"
#include "cloud_roles/apply_credentials.h"
#include "cloud_storage_clients/client.h"
#include "cloud_storage_clients/client_probe.h"
#include "cloud_storage_clients/types.h"
#include "http/client.h"
#include "model/fundamental.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>

#include <chrono>
#include <initializer_list>
#include <span>
#include <string>
#include <string_view>

namespace cloud_storage_clients {

/// Request formatter for AWS S3
class request_creator {
public:
    /// C-tor
    /// \param conf is a configuration container
    explicit request_creator(
      const s3_configuration& conf,
      ss::lw_shared_ptr<const cloud_roles::apply_credentials>
        apply_credentials);

    /// \brief Create unsigned 'PutObject' request header
    /// The payload is unsigned which means that we don't need to calculate
    /// hash from it (which don't want to do for large files).
    ///
    /// \param name is a bucket that should be used to store new object
    /// \param key is an object name
    /// \param payload_size_bytes is a size of the object in bytes
    /// \return initialized and signed http header or error
    result<http::client::request_header> make_unsigned_put_object_request(
      const bucket_name& name,
      const object_key& key,
      size_t payload_size_bytes,
      header_map_t headers = {});

    /// \brief Create a 'GetObject' request header
    ///
    ///
    /// \param name is a bucket that has the object
    /// \param key is an object name
    /// \return initialized and signed http header or error
    result<http::client::request_header> make_get_object_request(
      const bucket_name& name,
      const object_key& key,
      std::optional<http_byte_range> byte_range = std::nullopt,
      header_map_t headers = {});

    /// \brief Create a 'HeadObject' request header
    ///
    /// \param name is a bucket that has the object
    /// \param key is an object name
    /// \return initialized and signed http header or error
    result<http::client::request_header> make_head_object_request(
      const bucket_name& name,
      const object_key& key,
      header_map_t headers = {});

    /// \brief Create a 'DeleteObject' request header
    ///
    /// \param name is a bucket that has the object
    /// \param key is an object name
    /// \return initialized and signed http header or error
    result<http::client::request_header>
    make_delete_object_request(const bucket_name& name, const object_key& key);

    /// \brief Create a 'DeleteObjects' request header and body
    ///
    /// \param name of the bucket
    /// \param keys to delete
    /// \return the header and an the body as an input_stream
    result<std::tuple<http::client::request_header, ss::input_stream<char>>>
    make_delete_objects_request(
      const bucket_name& name, std::span<const object_key> keys);

    /// \brief Initialize http header for 'ListObjectsV2' request
    ///
    /// \param name of the bucket
    /// \param region to connect
    /// \param max_keys is a max number of returned objects
    /// \param offset is an offset of the first returned object
    /// \param continuation_token used to paginate list results
    /// \param delimiter used to group results with common prefixes
    /// \return initialized and signed http header or error
    result<http::client::request_header> make_list_objects_v2_request(
      const bucket_name& name,
      std::optional<object_key> prefix,
      std::optional<object_key> start_after,
      std::optional<size_t> max_keys,
      std::optional<ss::sstring> continuation_token,
      std::optional<char> delimiter = std::nullopt);

private:
    friend class s3_client;
    std::string make_host(const bucket_name& name) const;

    std::string
    make_target(const bucket_name& name, const object_key& key) const;

    access_point_uri _ap;

    s3_url_style _ap_style;
    /// Applies credentials to http requests by adding headers and signing
    /// request payload. Shared pointer so that the credentials can be rotated
    /// through the client pool.
    ss::lw_shared_ptr<const cloud_roles::apply_credentials> _apply_credentials;
};

/// S3 REST-API client
class s3_client : public client {
public:
    s3_client(
      const s3_configuration& conf,
      ss::lw_shared_ptr<const cloud_roles::apply_credentials>
        apply_credentials);
    s3_client(
      const s3_configuration& conf,
      const ss::abort_source& as,
      ss::lw_shared_ptr<const cloud_roles::apply_credentials>
        apply_credentials);

    ss::future<result<client_self_configuration_output, error_outcome>>
    self_configure() override;

    /// Stop the client
    ss::future<> stop() override;
    /// Shutdown the underlying connection
    void shutdown() override;

    /// Download object from S3 bucket
    ///
    /// \param name is a bucket name
    /// \param key is an object key
    /// \param timeout is a timeout of the operation
    /// \param expect_no_such_key log 404 as warning if set to false
    /// \return future that gets ready after request was sent
    ss::future<result<http::client::response_stream_ref, error_outcome>>
    get_object(
      const bucket_name& name,
      const object_key& key,
      ss::lowres_clock::duration timeout,
      bool expect_no_such_key = false,
      std::optional<http_byte_range> byte_range = std::nullopt,
      header_map_t headers = {}) override;

    /// HeadObject request.
    /// \param name is a bucket name
    /// \param key is an id of the object
    /// \return future that becomes ready when the request is completed
    ss::future<result<head_object_result, error_outcome>> head_object(
      const bucket_name& name,
      const object_key& key,
      ss::lowres_clock::duration timeout,
      header_map_t headers = {}) override;

    /// Put object to S3 bucket.
    /// \param name is a bucket name
    /// \param key is an id of the object
    /// \param payload_size is a size of the object in bytes
    /// \param body is an input_stream that can be used to read body
    /// \return future that becomes ready when the upload is completed
    ss::future<result<no_response, error_outcome>> put_object(
      const bucket_name& name,
      const object_key& key,
      size_t payload_size,
      ss::input_stream<char> body,
      ss::lowres_clock::duration timeout,
      bool accept_no_content = false,
      header_map_t headers = {}) override;

    ss::future<result<list_bucket_result, error_outcome>> list_objects(
      const bucket_name& name,
      std::optional<object_key> prefix = std::nullopt,
      std::optional<object_key> start_after = std::nullopt,
      std::optional<size_t> max_keys = std::nullopt,
      std::optional<ss::sstring> continuation_token = std::nullopt,
      ss::lowres_clock::duration timeout = http::default_connect_timeout,
      std::optional<char> delimiter = std::nullopt,
      std::optional<item_filter> collect_item_if = std::nullopt) override;

    ss::future<result<no_response, error_outcome>> delete_object(
      const bucket_name& bucket,
      const object_key& key,
      ss::lowres_clock::duration timeout) override;

    ss::future<result<delete_objects_result, error_outcome>> delete_objects(
      const bucket_name& bucket,
      std::vector<object_key> keys,
      ss::lowres_clock::duration timeout) override;

private:
    ss::future<head_object_result> do_head_object(
      const bucket_name& name,
      const object_key& key,
      ss::lowres_clock::duration timeout,
      header_map_t headers = {});

    ss::future<http::client::response_stream_ref> do_get_object(
      const bucket_name& name,
      const object_key& key,
      ss::lowres_clock::duration timeout,
      bool expect_no_such_key = false,
      std::optional<http_byte_range> byte_range = std::nullopt,
      header_map_t headers = {});

    ss::future<> do_put_object(
      const bucket_name& name,
      const object_key& key,
      size_t payload_size,
      ss::input_stream<char> body,
      ss::lowres_clock::duration timeout,
      bool accept_no_content = false,
      header_map_t headers = {});

    ss::future<list_bucket_result> do_list_objects_v2(
      const bucket_name& name,
      std::optional<object_key> prefix = std::nullopt,
      std::optional<object_key> start_after = std::nullopt,
      std::optional<size_t> max_keys = std::nullopt,
      std::optional<ss::sstring> continuation_token = std::nullopt,
      ss::lowres_clock::duration timeout = http::default_connect_timeout,
      std::optional<char> delimiter = std::nullopt,
      std::optional<item_filter> collect_item_if = std::nullopt);

    ss::future<> do_delete_object(
      const bucket_name& bucket,
      const object_key& key,
      ss::lowres_clock::duration timeout);

    ss::future<delete_objects_result> do_delete_objects(
      const bucket_name& bucket,
      std::span<const object_key> keys,
      ss::lowres_clock::duration timeout);

    template<typename T>
    ss::future<result<T, error_outcome>> send_request(
      ss::future<T> request_future,
      const bucket_name& bucket,
      const object_key& key);

    // Performs testing as part of the self-configuration step. Returns true if
    // the test was successful (indicating that the current addressing style is
    // compatible with the configured cloud storage provider), and false
    // otherwise.
    ss::future<bool> self_configure_test(const bucket_name& bucket);

private:
    request_creator _requestor;
    http::client _client;
    ss::shared_ptr<client_probe> _probe;
};

std::variant<client::delete_objects_result, rest_error_response>
iobuf_to_delete_objects_result(iobuf&& buf);

} // namespace cloud_storage_clients
