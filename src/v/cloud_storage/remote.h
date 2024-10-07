/*
 * Copyright 2021 Redpanda Data, Inc.
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
#include "cloud_io/remote.h"
#include "cloud_roles/refresh_credentials.h"
#include "cloud_storage/base_manifest.h"
#include "cloud_storage/configuration.h"
#include "cloud_storage/fwd.h"
#include "cloud_storage/read_path_probes.h"
#include "cloud_storage/remote_probe.h"
#include "cloud_storage/remote_segment_index.h"
#include "cloud_storage/types.h"
#include "cloud_storage_clients/client.h"
#include "cloud_storage_clients/client_pool.h"
#include "cloud_storage_clients/types.h"
#include "container/intrusive_list_helpers.h"
#include "model/metadata.h"
#include "random/simple_time_jitter.h"
#include "utils/lazy_abort_source.h"
#include "utils/retry_chain_node.h"
#include "utils/stream_provider.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>

#include <ranges>
#include <utility>

namespace cloud_storage {

class materialized_resources;

inline constexpr ss::shard_id auth_refresh_shard_id = 0;

enum class api_activity_type {
    segment_upload,
    segment_download,
    segment_delete,
    manifest_upload,
    manifest_download,
    controller_snapshot_upload,
    controller_snapshot_download,
    object_upload,
    object_download
};

struct api_activity_notification {
    api_activity_type type;
    bool is_retry;
};

// Enables creating mocks from remote. A mock class should be extended from this
// interface in tests, and methods in real code should accept
// references/pointers to this interface instead of remote to enable testing.
class cloud_storage_api {
public:
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

    using latency_measurement_t
      = std::unique_ptr<remote_probe::hist_t::measurement>;
    struct download_metrics {
        std::function<latency_measurement_t()> download_latency_measurement =
          [] { return nullptr; };
        std::function<void()> failed_download_metric = [] {};
        std::function<void()> download_backoff_metric = [] {};
    };

    virtual ss::future<upload_result> upload_object(upload_request) = 0;
    virtual ss::future<download_result> download_object(download_request) = 0;
    virtual ss::future<list_result> list_objects(
      const cloud_storage_clients::bucket_name&,
      retry_chain_node&,
      std::optional<cloud_storage_clients::object_key> = std::nullopt,
      std::optional<char> = std::nullopt,
      std::optional<cloud_storage_clients::client::item_filter> = std::nullopt,
      std::optional<size_t> = std::nullopt,
      std::optional<ss::sstring> = std::nullopt)
      = 0;
    virtual ss::future<download_result> object_exists(
      const cloud_storage_clients::bucket_name&,
      const cloud_storage_clients::object_key&,
      retry_chain_node&,
      existence_check_type)
      = 0;
    virtual ss::future<download_result> download_stream(
      const cloud_storage_clients::bucket_name& bucket,
      const remote_segment_path& path,
      const try_consume_stream& cons_str,
      retry_chain_node& parent,
      const std::string_view stream_label,
      const download_metrics& metrics,
      std::optional<cloud_storage_clients::http_byte_range> byte_range
      = std::nullopt)
      = 0;
    virtual ~cloud_storage_api() = default;
};

/// \brief Represents remote endpoint
///
/// The `remote` is responsible for remote data
/// transfer and storage. It 'knows' how to upload and
/// download data. Also, it's responsible for maintaining
/// correct naming in S3. The remote takes into account
/// things like reconnects, backpressure and backoff.
class remote : public cloud_storage_api {
public:
    /// Functor that returns fresh input_stream object that can be used
    /// to re-upload and will return all data that needs to be uploaded
    using reset_input_stream = ss::noncopyable_function<
      ss::future<std::unique_ptr<stream_provider>>()>;

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
      ss::sharded<cloud_io::remote>& io,
      const cloud_storage_clients::client_configuration& conf);

    ~remote() override;

    /// \brief Initialize 'remote'
    ///
    /// \param conf is an archival configuration
    explicit remote(
      ss::sharded<cloud_io::remote>& io, const configuration& conf);

    /// \brief Start the remote
    ss::future<> start();

    /// \brief Stop the remote
    ///
    /// Wait until all background operations complete
    ss::future<> stop();

    /// Return max number of concurrent requests that the object
    /// can perform.
    size_t concurrency() const;

    bool is_batch_delete_supported() const;
    int delete_objects_max_keys() const;

    /// \brief Download manifest from pre-defined S3 location
    ///
    /// Method downloads the manifest and handles backpressure and
    /// errors. It retries multiple times until timeout excedes.
    /// \param bucket is a bucket name
    /// \param key is an object key of the manifest
    /// \param manifest is a manifest to download
    /// \return future that returns success code
    ss::future<download_result> download_manifest(
      const cloud_storage_clients::bucket_name& bucket,
      const std::pair<manifest_format, remote_manifest_path>& format_key,
      base_manifest& manifest,
      retry_chain_node& parent);

    /// compatibility version of download_manifest for json format
    ss::future<download_result> download_manifest_json(
      const cloud_storage_clients::bucket_name& bucket,
      const remote_manifest_path& key,
      base_manifest& manifest,
      retry_chain_node& parent);
    /// version of download_manifest for serde format
    ss::future<download_result> download_manifest_bin(
      const cloud_storage_clients::bucket_name& bucket,
      const remote_manifest_path& key,
      base_manifest& manifest,
      retry_chain_node& parent);

    /// \brief Download manifest from pre-defined S3 location
    ///
    /// Method downloads the manifest and handles backpressure and
    /// errors. It retries multiple times until timeout excedes.
    /// The method expects that the manifest might be missing from
    /// S3 bucket. It behaves exactly the same as 'download_manifest'.
    /// The only difference is that 'NoSuchKey' error is not logged
    /// using 'warn' log level.
    ///
    /// \param bucket is a bucket name
    /// \param key is an object key of the manifest
    /// \param manifest is a manifest to download
    /// \return future that returns success code
    ss::future<download_result> maybe_download_manifest(
      const cloud_storage_clients::bucket_name& bucket,
      const remote_manifest_path& key,
      base_manifest& manifest,
      retry_chain_node& parent);

    /// \brief Upload manifest to the pre-defined S3 location
    ///
    /// \param bucket is a bucket name
    /// \param manifest is a manifest to upload
    /// \param key is the remote object name
    /// \return future that returns success code
    ss::future<upload_result> upload_manifest(
      const cloud_storage_clients::bucket_name& bucket,
      const base_manifest& manifest,
      const remote_manifest_path& key,
      retry_chain_node& parent);

    /// \brief Upload segment to S3
    ///
    /// The method uploads the segment while tolerating some errors. It can
    /// retry after some errors.
    /// \param reset_str is a functor that returns an input_stream that returns
    ///                  segment's data
    /// \param exposed_name is a segment's name in S3
    /// \param manifest is a manifest that should have the segment metadata
    /// \param max_retries is a maximal number of allowed retries
    ss::future<upload_result> upload_segment(
      const cloud_storage_clients::bucket_name& bucket,
      const remote_segment_path& segment_path,
      uint64_t content_length,
      const reset_input_stream& reset_str,
      retry_chain_node& parent,
      lazy_abort_source& lazy_abort_source,
      std::optional<size_t> max_retries = std::nullopt);

    /// \brief Upload segment index to the pre-defined S3 location
    ///
    /// \param bucket is a bucket name
    /// \param manifest is the index to upload
    /// \param key is the remote object name
    /// \return future that returns success code
    ss::future<upload_result> upload_index(
      const cloud_storage_clients::bucket_name& bucket,
      const cloud_storage_clients::object_key& key,
      const offset_index& index,
      retry_chain_node& parent);

    /// \brief Download segment from S3
    ///
    /// The method downloads the segment while tolerating some errors. It can
    /// retry after an error.
    /// \param cons_str is a functor that consumes an input_stream with
    /// segment's data
    /// \param name is a segment's name in S3
    /// \param manifest is a manifest that should have the segment metadata
    ss::future<download_result> download_segment(
      const cloud_storage_clients::bucket_name& bucket,
      const remote_segment_path& path,
      const try_consume_stream& cons_str,
      retry_chain_node& parent,
      std::optional<cloud_storage_clients::http_byte_range> byte_range
      = std::nullopt);

    /// \brief Upload a controller snapshot file to S3
    ///
    /// The method uploads the snapshot while tolerating some errors. It can
    /// retry after some errors.
    /// \param remote_path is a snapshot's name in S3
    /// \param file is a controller snapshot file
    /// \param parent is used for logging and retries
    /// \param lazy_abort_source is used to stop further upload attempts
    ss::future<upload_result> upload_controller_snapshot(
      const cloud_storage_clients::bucket_name& bucket,
      const remote_segment_path& remote_path,
      const ss::file& file,
      retry_chain_node& parent,
      lazy_abort_source& lazy_abort_source);

    /// \brief Download stream from S3
    ///
    /// The method downloads the segment while tolerating some errors. It can
    /// retry after an error.
    /// \param bucket is the remote bucket
    /// \param path is an object name in S3
    /// \param cons_str is a functor that consumes an input_stream
    /// \param parent is used for logging and retries
    /// \stream_label the type of stream, used for logging
    /// \metrics download-related metric functions
    /// \byte_range the range in the stream to download
    ss::future<download_result> download_stream(
      const cloud_storage_clients::bucket_name& bucket,
      const remote_segment_path& path,
      const try_consume_stream& cons_str,
      retry_chain_node& parent,
      const std::string_view stream_label,
      const download_metrics& metrics,
      std::optional<cloud_storage_clients::http_byte_range> byte_range
      = std::nullopt) override;

    /// \brief Download segment index from S3
    /// \param ix is the index which will be populated from data from the object
    /// store
    ss::future<download_result> download_index(
      const cloud_storage_clients::bucket_name& bucket,
      const remote_segment_path& index_path,
      offset_index& ix,
      retry_chain_node& parent);

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
      existence_check_type object_type) override;

    /// Checks if the segment exists in the bucket
    ss::future<download_result> segment_exists(
      const cloud_storage_clients::bucket_name& bucket,
      const remote_segment_path& path,
      retry_chain_node& parent);

    /// \brief Delete object from S3
    ///
    /// The method deletes the object. It can retry after some errors.
    ///
    /// \param path is a full S3 object path
    /// \param bucket is a name of the S3 bucket
    ss::future<upload_result> delete_object(
      const cloud_storage_clients::bucket_name& bucket,
      const cloud_storage_clients::object_key& path,
      retry_chain_node& parent);

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
      retry_chain_node& parent);

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
      std::optional<ss::sstring> continuation_token = std::nullopt) override;

    /// \brief Upload small objects to bucket. Suitable for uploading simple
    /// strings, does not check for leadership before upload like the segment
    /// upload function.
    ss::future<upload_result>
    upload_object(upload_request upload_request) override;

    ss::future<download_result> do_download_manifest(
      const cloud_storage_clients::bucket_name& bucket,
      const std::pair<manifest_format, remote_manifest_path>& format_key,
      base_manifest& manifest,
      retry_chain_node& parent,
      bool expect_missing = false);

    materialized_resources& materialized() { return *_materialized; }

    /// Event filter class.
    ///
    /// The filter can be used to subscribe to subset of events.
    /// For instance, only to segment downloads and uploads, or to
    /// events from all sybsystems except one.
    /// The filter is a RAII object. It works until the object
    /// exists. If the filter is destroyed before the notification
    /// will be received the receiver of the event will see broken
    /// promise error.
    class event_filter {
        friend class remote;

    public:
        event_filter() = default;

        explicit event_filter(
          std::unordered_set<api_activity_type> ignored_events)
          : _events_to_ignore(std::move(ignored_events)) {}

        void add_source_to_ignore(const retry_chain_node* source) {
            _sources_to_ignore.insert(source);
        }

        void remove_source_to_ignore(const retry_chain_node* source) {
            _sources_to_ignore.erase(source);
        }

        void cancel() {
            if (_promise.has_value()) {
                _hook.unlink();
                _promise.reset();
            }
        }

    private:
        absl::node_hash_set<const retry_chain_node*> _sources_to_ignore;
        std::unordered_set<api_activity_type> _events_to_ignore;
        std::optional<ss::promise<api_activity_notification>> _promise;
        intrusive_list_hook _hook;
    };

    /// Return future that will become available on next cloud storage
    /// api operation.
    ///
    /// \note The operations which are trigger notifications are segment upload,
    /// segment download, segment(s) delete, manifest upload, manifest download.
    /// The notification is generated before the actual use and does not
    /// affected by errors. The notification is generated even if the operation
    /// failed. Also, every retry is generating its own notification.
    ///
    /// \param filter is a notification filter which allows to narrow the set of
    ///        posible notificatoins by source and type.
    /// \return the future which will be available after the next cloud storage
    ///         API operation.
    ss::future<api_activity_notification> subscribe(event_filter& filter);

    // If you need to spawn a background task that relies on
    // this object staying alive, spawn it with this gate.
    seastar::gate& gate() { return io().gate(); };
    ss::abort_source& as() { return io().as(); }

    remote_probe& get_probe() { return _probe; }

private:
    cloud_io::remote& io() { return _io.local(); }
    const cloud_io::remote& io() const { return _io.local(); }

    /// Notify all subscribers about segment or manifest upload/download
    void notify_external_subscribers(
      api_activity_notification, const retry_chain_node& caller);
    std::function<void(size_t)>
    make_notify_cb(api_activity_type t, retry_chain_node& retry);

    ss::sharded<cloud_io::remote>& _io;
    std::unique_ptr<materialized_resources> _materialized;

    // Lifetime: probe has reference to _materialized, must be destroyed after
    remote_probe _probe;

    intrusive_list<event_filter, &event_filter::_hook> _filters;
};

} // namespace cloud_storage
