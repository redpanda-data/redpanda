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

#include "cloud_roles/refresh_credentials.h"
#include "cloud_storage/base_manifest.h"
#include "cloud_storage/fwd.h"
#include "cloud_storage/probe.h"
#include "cloud_storage/remote_segment_index.h"
#include "cloud_storage/types.h"
#include "cloud_storage_clients/client.h"
#include "cloud_storage_clients/client_pool.h"
#include "model/metadata.h"
#include "random/simple_time_jitter.h"
#include "storage/segment_reader.h"
#include "utils/intrusive_list_helpers.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>

#include <utility>

namespace cloud_storage {

class materialized_segments;

/// \brief Predicate required to continue operation
///
/// Describes a predicate to be evaluated before starting an expensive
/// operation, or to be evaluated when an operation has failed, to find
/// the reason for failure
struct lazy_abort_source {
    /// Predicate to be evaluated before an operation. Evaluates to true when
    /// the operation should be aborted, false otherwise.
    using predicate_t = ss::noncopyable_function<std::optional<ss::sstring>()>;

    lazy_abort_source(predicate_t predicate)
      : _predicate{std::move(predicate)} {}

    bool abort_requested();
    ss::sstring abort_reason() const;

private:
    ss::sstring _abort_reason;
    predicate_t _predicate;
};

static constexpr ss::shard_id auth_refresh_shard_id = 0;

/// Helper class to start the background operations to periodically refresh
/// authentication. Selects the implementation for fetch based on the
/// cloud_credentials_source property.
class auth_refresh_bg_op {
public:
    auth_refresh_bg_op(
      ss::gate& gate,
      ss::abort_source& as,
      cloud_storage_clients::client_configuration client_conf,
      model::cloud_credentials_source cloud_credentials_source);

    /// Helper to decide if credentials will be regularly fetched from
    /// infrastructure APIs or loaded once from config file.
    bool is_static_config() const;

    /// Builds a set of static AWS compatible credentials, reading values from
    /// the S3 configuration passed to us.
    cloud_roles::credentials build_static_credentials() const;

    /// Start a background refresh operation, accepting a callback which is
    /// called with newly fetched credentials periodically. The operation is
    /// started on auth_refresh_shard_id and credentials are copied to other
    /// shards using the callback.
    void maybe_start_auth_refresh_op(
      cloud_roles::credentials_update_cb_t credentials_update_cb);

    cloud_storage_clients::client_configuration get_client_config() const;
    void set_client_config(cloud_storage_clients::client_configuration conf);

    ss::future<> stop();

private:
    void do_start_auth_refresh_op(
      cloud_roles::credentials_update_cb_t credentials_update_cb);

    ss::gate& _gate;
    ss::abort_source& _as;
    cloud_storage_clients::client_configuration _client_conf;
    model::cloud_credentials_source _cloud_credentials_source;
    std::optional<cloud_roles::refresh_credentials> _refresh_credentials;
};

enum class api_activity_notification {
    segment_upload,
    segment_download,
    segment_delete,
    manifest_upload,
    manifest_download,
};

/// \brief Represents remote endpoint
///
/// The `remote` is responsible for remote data
/// transfer and storage. It 'knows' how to upload and
/// download data. Also, it's responsible for maintaining
/// correct naming in S3. The remote takes into account
/// things like reconnects, backpressure and backoff.
class remote : public ss::peering_sharded_service<remote> {
public:
    /// Default tags applied to objects
    static const cloud_storage_clients::object_tag_formatter
      default_segment_tags;
    static const cloud_storage_clients::object_tag_formatter
      default_partition_manifest_tags;
    static const cloud_storage_clients::object_tag_formatter
      default_topic_manifest_tags;
    static const cloud_storage_clients::object_tag_formatter default_index_tags;

    /// Functor that returns fresh input_stream object that can be used
    /// to re-upload and will return all data that needs to be uploaded
    using reset_input_stream = ss::noncopyable_function<
      ss::future<std::unique_ptr<storage::stream_provider>>()>;

    /// Functor that attempts to consume the input stream. If the connection
    /// is broken during the download the functor is responsible for he cleanup.
    /// The functor should be reenterable since it can be called many times.
    /// On success it should return content_length. On failure it should
    /// allow the exception from the input_stream to propagate.
    using try_consume_stream = ss::noncopyable_function<ss::future<uint64_t>(
      uint64_t, ss::input_stream<char>)>;

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

    ~remote();

    /// \brief Initialize 'remote'
    ///
    /// \param conf is an archival configuration
    explicit remote(
      ss::sharded<cloud_storage_clients::client_pool>& pool,
      const configuration& conf);

    /// \brief Start the remote
    ss::future<> start();

    /// \brief Stop the remote
    ///
    /// Wait until all background operations complete
    ss::future<> stop();

    /// Return max number of concurrent requests that the object
    /// can perform.
    size_t concurrency() const;

    model::cloud_storage_backend backend() const;

    bool is_batch_delete_supported() const;

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
    /// \return future that returns success code
    ss::future<upload_result> upload_manifest(
      const cloud_storage_clients::bucket_name& bucket,
      const base_manifest& manifest,
      retry_chain_node& parent,
      const cloud_storage_clients::object_tag_formatter& tags
      = default_partition_manifest_tags);

    /// \brief Upload segment to S3
    ///
    /// The method uploads the segment while tolerating some errors. It can
    /// retry after some errors.
    /// \param reset_str is a functor that returns an input_stream that returns
    ///                  segment's data
    /// \param exposed_name is a segment's name in S3
    /// \param manifest is a manifest that should have the segment metadata
    ss::future<upload_result> upload_segment(
      const cloud_storage_clients::bucket_name& bucket,
      const remote_segment_path& segment_path,
      uint64_t content_length,
      const reset_input_stream& reset_str,
      retry_chain_node& parent,
      lazy_abort_source& lazy_abort_source,
      const cloud_storage_clients::object_tag_formatter& tags
      = default_segment_tags);

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
      retry_chain_node& parent);

    /// \brief Download segment index from S3
    /// \param ix is the index which will be populated from data from the object
    /// store
    ss::future<download_result> download_index(
      const cloud_storage_clients::bucket_name& bucket,
      const remote_segment_path& index_path,
      offset_index& ix,
      retry_chain_node& parent);

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
    /// \param keys A vector of keys which will be deleted
    ss::future<upload_result> delete_objects(
      const cloud_storage_clients::bucket_name& bucket,
      std::vector<cloud_storage_clients::object_key> keys,
      retry_chain_node& parent);

    using list_result = result<
      cloud_storage_clients::client::list_bucket_result,
      cloud_storage_clients::error_outcome>;

    /// \brief Lists objects in a bucket
    ///
    /// \param name The bucket to delete from
    /// \param parent The retry chain node to manage timeouts
    /// \param prefix Optional prefix to restrict listing of objects
    /// \param delimiter A character to use as a delimiter when grouping list
    /// results
    /// \param item_filter Optional filter to apply to items before
    /// collecting
    ss::future<list_result> list_objects(
      const cloud_storage_clients::bucket_name& name,
      retry_chain_node& parent,
      std::optional<cloud_storage_clients::object_key> prefix = std::nullopt,
      std::optional<char> delimiter = std::nullopt,
      std::optional<cloud_storage_clients::client::item_filter> item_filter
      = std::nullopt);

    /// \brief Upload small objects to bucket. Suitable for uploading simple
    /// strings, does not check for leadership before upload like the segment
    /// upload function.
    ///
    /// \param bucket The bucket to upload to
    /// \param object_path The path to upload to
    /// \param payload The data to place in the bucket
    ss::future<upload_result> upload_object(
      const cloud_storage_clients::bucket_name& bucket,
      const cloud_storage_clients::object_key& object_path,
      iobuf payload,
      retry_chain_node& parent,
      const cloud_storage_clients::object_tag_formatter& tags,
      const char* log_object_type = "object");

    ss::future<download_result> do_download_manifest(
      const cloud_storage_clients::bucket_name& bucket,
      const remote_manifest_path& key,
      base_manifest& manifest,
      retry_chain_node& parent,
      bool expect_missing = false);

    materialized_segments& materialized() { return *_materialized; }

    /// Event filter class.
    ///
    /// The filter can be used to subscribe to subset of events.
    /// For instance, only to segment downloads and uploads, or to
    /// events from all sybsystems except one.
    /// The filter is a RAII object. It works until the object
    /// exists. If the filter is destroyed before the notification
    /// will be received the receiver of the event will se broken
    /// promise error.
    class event_filter {
        friend class remote;

    public:
        /// Event filter that subscribes to events from all sources.
        /// The event type wildcard can also be specified.
        explicit event_filter(
          std::unordered_set<api_activity_notification> ignored_events = {})
          : _events_to_ignore(std::move(ignored_events)) {}
        /// Event filter that subscribes to events from all sources
        /// except one. The event type wildcard can also be specified.
        /// The filter will ignore all events triggered by callers which
        /// are using the same retry_chain_node. This is useful when the
        /// client of the 'remote' needs to subscribe to all events except
        /// own.
        explicit event_filter(
          retry_chain_node& ignored_src,
          std::unordered_set<api_activity_notification> ignored_events = {})
          : _source_to_ignore(std::ref(ignored_src))
          , _events_to_ignore(std::move(ignored_events)) {}

        void cancel() {
            if (_promise.has_value()) {
                _hook.unlink();
                _promise.reset();
            }
        }

    private:
        std::optional<std::reference_wrapper<retry_chain_node>>
          _source_to_ignore;
        std::unordered_set<api_activity_notification> _events_to_ignore;
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

    /// Add partition manifest tags (includes partition id)
    static cloud_storage_clients::object_tag_formatter
    make_partition_manifest_tags(
      const model::ntp& ntp, model::initial_revision_id rev);
    /// Add topic manifest tags (no partition id)
    static cloud_storage_clients::object_tag_formatter make_topic_manifest_tags(
      const model::topic_namespace& ntp, model::initial_revision_id rev);
    /// Add segment level tags
    static cloud_storage_clients::object_tag_formatter
    make_segment_tags(const model::ntp& ntp, model::initial_revision_id rev);
    /// Add tags for tx-manifest
    static cloud_storage_clients::object_tag_formatter make_tx_manifest_tags(
      const model::ntp& ntp, model::initial_revision_id rev);

    static cloud_storage_clients::object_tag_formatter make_segment_index_tags(
      const model::ntp& ntp, model::initial_revision_id rev);

private:
    ss::future<upload_result> delete_objects_sequentially(
      const cloud_storage_clients::bucket_name& bucket,
      std::vector<cloud_storage_clients::object_key> keys,
      retry_chain_node& parent);

    ss::future<> propagate_credentials(cloud_roles::credentials credentials);
    /// Notify all subscribers about segment or manifest upload/download
    void notify_external_subscribers(
      api_activity_notification, const retry_chain_node& caller);

    ss::sharded<cloud_storage_clients::client_pool>& _pool;
    ss::gate _gate;
    ss::abort_source _as;
    auth_refresh_bg_op _auth_refresh_bg_op;
    std::unique_ptr<materialized_segments> _materialized;

    // Lifetime: probe has reference to _materialized, must be destroyed after
    remote_probe _probe;

    intrusive_list<event_filter, &event_filter::_hook> _filters;

    config::binding<std::optional<ss::sstring>> _azure_shared_key_binding;

    model::cloud_storage_backend _cloud_storage_backend;
};

} // namespace cloud_storage
