/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_io/auth_refresh_bg_op.h"
#include "cloud_io/io_resources.h"
#include "cloud_io/io_result.h"
#include "cloud_io/transfer_details.h"
#include "cloud_roles/refresh_credentials.h"
#include "cloud_storage_clients/client.h"
#include "cloud_storage_clients/client_pool.h"
#include "model/metadata.h"
#include "utils/lazy_abort_source.h"
#include "utils/retry_chain_node.h"
#include "utils/stream_provider.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/noncopyable_function.hh>

#include <ranges>
#include <utility>

namespace cloud_io {

template<class Clock>
struct basic_upload_request {
    basic_transfer_details<Clock> transfer_details;
    std::string_view display_str;
    iobuf payload;
    bool accept_no_content_response{false};
};
using upload_request = basic_upload_request<ss::lowres_clock>;

template<class Clock>
struct basic_download_request {
    basic_transfer_details<Clock> transfer_details;
    std::string_view display_str;
    iobuf& payload;
    bool expect_missing{false};
};
using download_request = basic_download_request<ss::lowres_clock>;
using remote_path = named_type<ss::sstring, struct remote_path_tag>;

using list_result = result<
  cloud_storage_clients::client::list_bucket_result,
  cloud_storage_clients::error_outcome>;

/// Functor that attempts to consume the input stream. If the connection
/// is broken during the download the functor is responsible for he cleanup.
/// The functor should be reenterable since it can be called many times.
/// On success it should return content_length. On failure it should
/// allow the exception from the input_stream to propagate.
using try_consume_stream = ss::noncopyable_function<ss::future<uint64_t>(
  uint64_t, ss::input_stream<char>)>;

template<class Clock = ss::lowres_clock>
class remote_api {
public:
    using upload_request = basic_upload_request<Clock>;
    using download_request = basic_download_request<Clock>;
    using retry_chain_node = basic_retry_chain_node<Clock>;
    using transfer_details = basic_transfer_details<Clock>;

    remote_api() = default;
    virtual ~remote_api() = default;
    remote_api(const remote_api&) = delete;
    remote_api(remote_api&&) = delete;
    remote_api& operator=(const remote_api&) = delete;
    remote_api& operator=(remote_api&&) = delete;

    /// Functor that returns fresh input_stream object that can be used
    /// to re-upload and will return all data that needs to be uploaded
    using reset_input_stream = ss::noncopyable_function<
      ss::future<std::unique_ptr<stream_provider>>()>;

    /// \brief Download object small enough to fit in memory
    /// \param download_request holds a reference to an iobuf in the `payload`
    /// field which will hold the downloaded object if the download was
    /// successful
    virtual ss::future<download_result>
    download_object(download_request download_request) = 0;

    virtual ss::future<download_result> object_exists(
      const cloud_storage_clients::bucket_name& bucket,
      const cloud_storage_clients::object_key& path,
      retry_chain_node& parent,
      std::string_view object_type)
      = 0;

    /// \brief Upload small objects to bucket. Suitable for uploading simple
    /// strings, does not check for leadership before upload like the segment
    /// upload function.
    virtual ss::future<upload_result>
    upload_object(upload_request upload_request) = 0;

    virtual ss::future<upload_result> upload_stream(
      transfer_details transfer_details,
      uint64_t content_length,
      const reset_input_stream& reset_str,
      lazy_abort_source& lazy_abort_source,
      const std::string_view stream_label,
      std::optional<size_t> max_retries)
      = 0;

    virtual ss::future<download_result> download_stream(
      transfer_details transfer_details,
      const try_consume_stream& cons_str,
      const std::string_view stream_label,
      bool acquire_hydration_units,
      std::optional<cloud_storage_clients::http_byte_range> byte_range
      = std::nullopt,
      std::function<void(size_t)> throttle_metric_ms_cb = {})
      = 0;
};

/// \brief Represents remote endpoint
///
/// The `remote` is responsible for remote data
/// transfer and storage. It 'knows' how to upload and
/// download data. Also, it's responsible for maintaining
/// correct naming in S3. The remote takes into account
/// things like reconnects, backpressure and backoff.
class remote final
  : public remote_api<>
  , public ss::peering_sharded_service<remote> {
public:
    /// Functor that should be provided by user when list_objects api is called.
    /// It receives every key that matches the query as well as it's modifiation
    /// time, size in bytes, and etag.
    using list_objects_consumer = std::function<ss::stop_iteration(
      ss::sstring, std::chrono::system_clock::time_point, size_t, ss::sstring)>;

    /// \brief Initialize 'remote'
    ///
    /// \param limit is a number of simultaneous connections
    /// \param conf is an S3 configuration
    remote(
      ss::sharded<cloud_storage_clients::client_pool>& clients,
      const cloud_storage_clients::client_configuration& conf,
      model::cloud_credentials_source cloud_credentials_source);

    ~remote() override;

    /// \brief Start the remote
    ss::future<> start();

    /// \brief Stop the remote
    ///
    /// Wait until all background operations complete
    void request_stop();
    ss::future<> stop();

    /// Return max number of concurrent requests that the object
    /// can perform.
    size_t concurrency() const;

    model::cloud_storage_backend backend() const;

    bool is_batch_delete_supported() const;
    int delete_objects_max_keys() const;

    /// \brief Download object small enough to fit in memory
    /// \param download_request holds a reference to an iobuf in the `payload`
    /// field which will hold the downloaded object if the download was
    /// successful
    ss::future<download_result>
    download_object(download_request download_request) override;

    ss::future<download_result> object_exists(
      const cloud_storage_clients::bucket_name& bucket,
      const cloud_storage_clients::object_key& path,
      retry_chain_node& parent,
      std::string_view object_type) override;

    /// \brief Delete object from S3
    ///
    /// The method deletes the object. It can retry after some errors.
    ///
    /// \param path is a full S3 object path
    /// \param bucket is a name of the S3 bucket
    ss::future<upload_result> delete_object(transfer_details);

    /// \brief Delete multiple objects from S3
    ///
    /// Deletes multiple objects from S3, utilizing the S3 client delete_objects
    /// API. For the backends where batch deletes are supported, batch deletes
    /// are performed. In other cases, sequential deletes are used as fallback.
    ///
    /// Note: the caller must ensure that if the backend does not support batch
    /// deletes, then the timeout or deadline set in parent should be sufficient
    /// to perform sequential deletes of the objects.
    ///
    /// \param bucket The bucket to delete from
    /// \param keys A range of keys which will be deleted
    template<typename R>
    requires std::ranges::range<R>
             && std::same_as<
               std::ranges::range_value_t<R>,
               cloud_storage_clients::object_key>
    ss::future<upload_result> delete_objects(
      const cloud_storage_clients::bucket_name& bucket,
      R keys,
      retry_chain_node& parent,
      std::function<void(size_t)> req_cb);

    /// \brief Lists objects in a bucket
    ///
    /// \param name The bucket to delete from
    /// \param parent The retry chain node to manage timeouts
    /// \param prefix Optional prefix to restrict listing of objects
    /// \param delimiter A character to use as a delimiter when grouping list
    /// results
    /// \param max_keys The maximum number of keys to return. If left
    /// unspecified, all object keys that fulfill the request will be collected,
    /// and the result will not be truncated (truncation not allowed). If
    /// specified, it will be up to the user to deal with a possibly-truncated
    /// result (using list_result.is_truncated) at the call site, most likely in
    /// a while loop. The continuation-token generated by that request will be
    /// available through list_result.next_continuation_token for future
    /// requests. It is also important to note that the value for max_keys will
    /// be capped by the cloud provider default (which may vary between
    /// providers, e.g AWS has a limit of 1000 keys per ListObjects request).
    /// \param continuation_token The token hopefully passed back to the user
    /// from a prior list_objects() request, in the case that they are handling
    /// a truncated result manually.
    /// \param item_filter Optional filter to apply to items before collecting
    ss::future<list_result> list_objects(
      const cloud_storage_clients::bucket_name& name,
      retry_chain_node& parent,
      std::optional<cloud_storage_clients::object_key> prefix = std::nullopt,
      std::optional<char> delimiter = std::nullopt,
      std::optional<cloud_storage_clients::client::item_filter> item_filter
      = std::nullopt,
      std::optional<size_t> max_keys = std::nullopt,
      std::optional<ss::sstring> continuation_token = std::nullopt);

    /// \brief Upload small objects to bucket. Suitable for uploading simple
    /// strings, does not check for leadership before upload like the segment
    /// upload function.
    ss::future<upload_result>
    upload_object(upload_request upload_request) override;

    // If you need to spawn a background task that relies on
    // this object staying alive, spawn it with this gate.
    seastar::gate& gate() { return _gate; };
    ss::abort_source& as() { return _as; }

    ss::future<upload_result> upload_stream(
      transfer_details transfer_details,
      uint64_t content_length,
      const reset_input_stream& reset_str,
      lazy_abort_source& lazy_abort_source,
      const std::string_view stream_label,
      std::optional<size_t> max_retries) override;

    ss::future<download_result> download_stream(
      transfer_details transfer_details,
      const try_consume_stream& cons_str,
      const std::string_view stream_label,
      bool acquire_hydration_units,
      std::optional<cloud_storage_clients::http_byte_range> byte_range
      = std::nullopt,
      std::function<void(size_t)> throttle_metric_ms_cb = {}) override;

    template<typename R>
    requires std::ranges::range<R>
             && std::same_as<
               std::ranges::range_value_t<R>,
               cloud_storage_clients::object_key>
    ss::future<upload_result> delete_objects_sequentially(
      const cloud_storage_clients::bucket_name& bucket,
      R keys,
      retry_chain_node& parent,
      std::function<void(size_t)> req_cb);

    /// Delete a single batch of keys. The batch size must not exceed the
    /// backend limits.
    ///
    /// \pre the number of keys is <= delete_objects_max_keys
    ss::future<upload_result> delete_object_batch(
      const cloud_storage_clients::bucket_name& bucket,
      std::vector<cloud_storage_clients::object_key> keys,
      retry_chain_node& parent,
      std::function<void(size_t)> req_cb);

    const io_resources& resources() const { return *_resources; }

private:
    ss::future<> propagate_credentials(cloud_roles::credentials credentials);

    ss::sharded<cloud_storage_clients::client_pool>& _pool;
    ss::gate _gate;
    ss::abort_source _as;
    auth_refresh_bg_op _auth_refresh_bg_op;
    std::unique_ptr<io_resources> _resources;

    config::binding<std::optional<ss::sstring>> _azure_shared_key_binding;

    model::cloud_storage_backend _cloud_storage_backend;
};

} // namespace cloud_io
