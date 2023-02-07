/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_roles/apply_credentials.h"
#include "cloud_storage_clients/client.h"
#include "cloud_storage_clients/client_probe.h"
#include "http/client.h"

namespace cloud_storage_clients {

/// Request formatter for Azure Blob Storage
class abs_request_creator {
public:
    /// C-tor
    /// \param conf is a configuration container
    explicit abs_request_creator(
      const abs_configuration& conf,
      ss::lw_shared_ptr<const cloud_roles::apply_credentials>
        apply_credentials);

    /// \brief Create 'Put Blob' request header
    ///
    /// \param name is container name
    /// \param key is the blob identifier
    /// \param payload_size_bytes is a size of the object in bytes
    /// \param payload_size_bytes is a size of the object in bytes
    /// \param tags are formatted tags for 'x-ms-tags'
    /// \return initialized and signed http header or error
    result<http::client::request_header> make_put_blob_request(
      bucket_name const& name,
      object_key const& key,
      size_t payload_size_bytes,
      const object_tag_formatter& tags);

    /// \brief Create a 'Get Blob' request header
    ///
    /// \param name is container name
    /// \param key is the blob identifier
    /// \return initialized and signed http header or error
    result<http::client::request_header>
    make_get_blob_request(bucket_name const& name, object_key const& key);

    /// \brief Create a 'Get Blob Metadata' request header
    ///
    /// \param name is a container
    /// \param key is a blob name
    /// \return initialized and signed http header or error
    result<http::client::request_header> make_get_blob_metadata_request(
      bucket_name const& name, object_key const& key);

    /// \brief Create a 'Delete Blob' request header
    ///
    /// \param name is a container
    /// \param key is an blob name
    /// \return initialized and signed http header or error
    result<http::client::request_header>
    make_delete_blob_request(bucket_name const& name, object_key const& key);

    /// \brief Initialize http header for 'List Blobs' request
    ///
    /// \param name of the container
    /// \param prefix prefix of returned blob's names
    /// \param start_after is always ignored
    /// \param max_keys is the max number of returned objects
    /// \param delimiter used to group common prefixes
    /// \return initialized and signed http header or error
    result<http::client::request_header> make_list_blobs_request(
      const bucket_name& name,
      std::optional<object_key> prefix,
      std::optional<object_key> start_after,
      std::optional<size_t> max_keys,
      std::optional<char> delimiter = std::nullopt);

private:
    access_point_uri _ap;
    /// Applies credentials to http requests by adding headers and signing
    /// request payload. Shared pointer so that the credentials can be
    /// rotated through the client pool.
    ss::lw_shared_ptr<const cloud_roles::apply_credentials> _apply_credentials;
};

/// S3 REST-API client
class abs_client : public client {
public:
    abs_client(
      const abs_configuration& conf,
      ss::lw_shared_ptr<const cloud_roles::apply_credentials>
        apply_credentials);

    abs_client(
      const abs_configuration& conf,
      const ss::abort_source& as,
      ss::lw_shared_ptr<const cloud_roles::apply_credentials>
        apply_credentials);

    /// Stop the client
    ss::future<> stop() override;

    /// Shutdown the underlying connection
    void shutdown() override;

    /// Download object from ABS container
    ///
    /// \param name is a container name
    /// \param key is a blob identifier
    /// \param timeout is a timeout of the operation
    /// \param expect_no_such_key log 404 as warning if set to false
    /// \return future that becomes ready after request was sent
    ss::future<result<http::client::response_stream_ref, error_outcome>>
    get_object(
      bucket_name const& name,
      object_key const& key,
      ss::lowres_clock::duration timeout,
      bool expect_no_such_key = false) override;

    /// Send Get Blob Metadata request.
    /// \param name is a container name
    /// \param key is an id of the blob
    /// \param timeout is a timeout of the operation
    /// \return future that becomes ready when the request is completed
    ss::future<result<head_object_result, error_outcome>> head_object(
      bucket_name const& name,
      object_key const& key,
      ss::lowres_clock::duration timeout) override;

    /// Put blob to ABS container.
    /// \param name is a container name
    /// \param key is an id of the blob
    /// \param payload_size is a size of the object in bytes
    /// \param body is an input_stream that can be used to read body
    /// \param timeout is a timeout of the operation
    /// \return future that becomes ready when the upload is completed
    ss::future<result<no_response, error_outcome>> put_object(
      bucket_name const& name,
      object_key const& key,
      size_t payload_size,
      ss::input_stream<char> body,
      const object_tag_formatter& tags,
      ss::lowres_clock::duration timeout) override;

    /// Send List Blobs request
    /// \param name is a container name
    /// \param prefix is an optional blob prefix to match
    /// \param start_after
    /// \param body is an input_stream that can be used to read body
    /// \param timeout is a timeout of the operation
    /// \return future that becomes ready when the request is completed
    ss::future<result<list_bucket_result, error_outcome>> list_objects(
      const bucket_name& name,
      std::optional<object_key> prefix = std::nullopt,
      std::optional<object_key> start_after = std::nullopt,
      std::optional<size_t> max_keys = std::nullopt,
      std::optional<ss::sstring> continuation_token = std::nullopt,
      ss::lowres_clock::duration timeout = http::default_connect_timeout,
      std::optional<char> delimiter = std::nullopt,
      std::optional<item_filter> collect_item_if = std::nullopt) override;

    /// Send Delete Blob request
    /// \param name is a container name
    /// \param key is an id of the blob
    /// \param timeout is a timeout of the operation
    ss::future<result<no_response, error_outcome>> delete_object(
      const bucket_name& bucket,
      const object_key& key,
      ss::lowres_clock::duration timeout) override;

    /// Issue multiple Delete Blob requests. Note that the timeout is used for
    /// each individual delete call, so there is a multiplication at work here.
    /// For example a timeout of 1 second with 100 objects to delete means that
    /// the timeout is expanded to 100 seconds.
    /// \param bucket is a container name
    /// \param keys is a list of blob ids
    /// \param timeout is a timeout of the operation
    ss::future<result<delete_objects_result, error_outcome>> delete_objects(
      const bucket_name& bucket,
      std::vector<object_key> keys,
      ss::lowres_clock::duration timeout) override;

private:
    template<typename T>
    ss::future<result<T, error_outcome>> send_request(
      ss::future<T> request_future,
      const bucket_name& bucket,
      const object_key& key,
      std::optional<op_type_tag> op_type = std::nullopt);

    ss::future<http::client::response_stream_ref> do_get_object(
      bucket_name const& name,
      object_key const& key,
      ss::lowres_clock::duration timeout,
      bool expect_no_such_key = false);

    ss::future<> do_put_object(
      bucket_name const& name,
      object_key const& key,
      size_t payload_size,
      ss::input_stream<char> body,
      const object_tag_formatter& tags,
      ss::lowres_clock::duration timeout);

    ss::future<head_object_result> do_head_object(
      bucket_name const& name,
      object_key const& key,
      ss::lowres_clock::duration timeout);

    ss::future<> do_delete_object(
      const bucket_name& name,
      const object_key& key,
      ss::lowres_clock::duration timeout);

    ss::future<list_bucket_result> do_list_objects(
      const bucket_name& name,
      std::optional<object_key> prefix,
      std::optional<object_key> start_after,
      std::optional<size_t> max_keys,
      std::optional<ss::sstring> continuation_token,
      ss::lowres_clock::duration timeout,
      std::optional<char> delimiter = std::nullopt,
      std::optional<item_filter> collect_item_if = std::nullopt);

    abs_request_creator _requestor;
    http::client _client;
    ss::shared_ptr<client_probe> _probe;
};

} // namespace cloud_storage_clients
