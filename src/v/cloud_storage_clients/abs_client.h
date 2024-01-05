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
    /// \return initialized and signed http header or error
    result<http::client::request_header> make_put_blob_request(
      bucket_name const& name,
      object_key const& key,
      size_t payload_size_bytes);

    /// \brief Create a 'Get Blob' request header
    ///
    /// \param name is container name
    /// \param key is the blob identifier
    /// \return initialized and signed http header or error
    result<http::client::request_header> make_get_blob_request(
      bucket_name const& name,
      object_key const& key,
      std::optional<http_byte_range> byte_range = std::nullopt);

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

    // clang-format off
    /// \brief Initialize http header for 'List Blobs' request
    ///
    /// \param name of the container
    /// \param files_only should always be set to true when HNS is enabled and false otherwise
    /// \param prefix prefix of returned blob's names
    /// \param start_after is always ignored \param max_keys is the max number of returned objects
    /// \param delimiter used to group common prefixes
    /// \return initialized and signed http header or error
    // clang-format on
    result<http::client::request_header> make_list_blobs_request(
      const bucket_name& name,
      bool files_only,
      std::optional<object_key> prefix,
      std::optional<object_key> start_after,
      std::optional<size_t> max_keys,
      std::optional<char> delimiter = std::nullopt);

    /// \brief Init http header for 'Get Account Information' request
    result<http::client::request_header> make_get_account_info_request();

    /// \brief Create a 'Filesystem Delete' request header
    ///
    /// \param adls_ap is the acces point for the Azure Data Lake Storage v2
    /// REST API
    /// \param name is a container
    /// \param key is a path
    /// \return initialized and signed http header or error
    result<http::client::request_header> make_delete_file_request(
      const access_point_uri& adls_ap,
      bucket_name const& name,
      object_key const& path);

    /// Unfortunately, Azure requires request payload size to be known in
    /// advance. Can't stream the request.
    /// https://learn.microsoft.com/en-us/rest/api/storageservices/blob-batch?tabs=azure-ad
    result<http::client::request_header>
    make_batch_request(const std::string& boundary, size_t content_length);

    result<http::client::request_header> make_batch_delete_blob_subrequest(
      bucket_name const& name, object_key const& key);

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

    ss::future<result<client_self_configuration_output, error_outcome>>
    self_configure() override;

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
      bool expect_no_such_key = false,
      std::optional<http_byte_range> byte_range = std::nullopt) override;

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

    struct storage_account_info {
        bool is_hns_enabled{false};
    };

    /// Send Get Account Information request (used to detect
    /// if the account has Hierarchical Namespace enabled).
    /// \param timeout is a timeout of the operation
    ss::future<result<storage_account_info, error_outcome>>
    get_account_info(ss::lowres_clock::duration timeout);

    /// Send Delete File request to ADLSv2 endpoint.
    ///
    /// Should only be called when Hierarchical Namespaces are
    /// enabled for the storage account.
    ///
    /// When HNS is enabled, the container has filesystem like semantics:
    /// deleting a blob, does not delete the path that leads up to it.
    /// This function deals with this by deleting elements from the path
    /// sequentially. For instance, if path=a/b/log.txt, we issue a separate
    /// delete requests for a/b/log.txt, a/b and a.
    ///
    /// \param name is a container name
    /// \param key is the path to be deleted
    /// \param timeout is a timeout of the operation
    ss::future<result<no_response, error_outcome>> delete_path(
      bucket_name const& name,
      object_key path,
      ss::lowres_clock::duration timeout);

private:
    template<typename T>
    ss::future<result<T, error_outcome>> send_request(
      ss::future<T> request_future,
      const object_key& key,
      std::optional<op_type_tag> op_type = std::nullopt);

    ss::future<http::client::response_stream_ref> do_get_object(
      bucket_name const& name,
      object_key const& key,
      ss::lowres_clock::duration timeout,
      bool expect_no_such_key = false,
      std::optional<http_byte_range> byte_range = std::nullopt);

    ss::future<> do_put_object(
      bucket_name const& name,
      object_key const& key,
      size_t payload_size,
      ss::input_stream<char> body,
      ss::lowres_clock::duration timeout);

    ss::future<head_object_result> do_head_object(
      bucket_name const& name,
      object_key const& key,
      ss::lowres_clock::duration timeout);

    ss::future<> do_delete_object(
      const bucket_name& name,
      const object_key& key,
      ss::lowres_clock::duration timeout);

    ss::future<result<abs_client::delete_objects_result, error_outcome>>
    do_delete_objects(
      const bucket_name& bucket,
      std::vector<object_key> paths,
      ss::lowres_clock::duration timeout);

    ss::future<iobuf> make_delete_objects_payload(
      const bucket_name& bucket,
      std::string boundary,
      const std::vector<object_key>& keys);

    ss::future<result<abs_client::delete_objects_result, error_outcome>>
    parse_delete_objects_payload(
      ss::input_stream<char> response_body,
       std::string_view boundary,
      const std::vector<object_key>& keys);

    ss::future<list_bucket_result> do_list_objects(
      const bucket_name& name,
      std::optional<object_key> prefix,
      std::optional<object_key> start_after,
      std::optional<size_t> max_keys,
      std::optional<ss::sstring> continuation_token,
      ss::lowres_clock::duration timeout,
      std::optional<char> delimiter = std::nullopt,
      std::optional<item_filter> collect_item_if = std::nullopt);

    ss::future<storage_account_info>
    do_get_account_info(ss::lowres_clock::duration timeout);

    ss::future<> do_delete_path(
      bucket_name const& name,
      object_key path,
      ss::lowres_clock::duration timeout);

    ss::future<result<abs_client::delete_objects_result, error_outcome>>
    do_delete_paths(
      const bucket_name& bucket,
      std::vector<object_key> paths,
      ss::lowres_clock::duration timeout);

    ss::future<> do_delete_file(
      const bucket_name& name,
      object_key path,
      ss::lowres_clock::duration timeout);

    std::optional<abs_configuration> _data_lake_v2_client_config;
    abs_request_creator _requestor;
    http::client _client;

    // Azure Storage accounts may have enabled Hierarchical Namespaces (HNS),
    // in which case the container will emulate file system like semantics.
    // For instance uploading blob "a/b/log.txt", creates two directory blobs
    // ("a" and "a/b") and one file blob ("a/b/log.txt").
    //
    // This changes the semantics of certain Blob Storage REST API requests:
    // * ListObjects will list both files and directories by default
    // * DeleteBlob cannot delete directory files
    //
    // `_adls_client` connects to the Azure Data Lake Storage V2 REST API
    // endpoint when HNS is enabled and is used for deletions.
    std::optional<http::client> _adls_client;

    ss::shared_ptr<client_probe> _probe;
};

} // namespace cloud_storage_clients
