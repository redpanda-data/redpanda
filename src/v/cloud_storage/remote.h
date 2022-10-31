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
#include "cloud_storage/types.h"
#include "random/simple_time_jitter.h"
#include "s3/client.h"
#include "storage/segment_reader.h"
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
    using predicate_t = ss::noncopyable_function<bool(lazy_abort_source&)>;

    lazy_abort_source(ss::sstring abort_reason, predicate_t predicate)
      : _abort_reason{std::move(abort_reason)}
      , _predicate{std::move(predicate)} {}

    bool abort_requested();

    ss::sstring abort_reason() const;

    void abort_reason(ss::sstring reason);

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
      s3::configuration s3_conf,
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

private:
    void do_start_auth_refresh_op(
      cloud_roles::credentials_update_cb_t credentials_update_cb);

    ss::gate& _gate;
    ss::abort_source& _as;
    s3::configuration _s3_conf;
    cloud_roles::aws_region_name _region_name;
    model::cloud_credentials_source _cloud_credentials_source;
    std::optional<cloud_roles::refresh_credentials> _refresh_credentials;
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
      s3_connection_limit limit,
      const s3::configuration& conf,
      model::cloud_credentials_source cloud_credentials_source);

    ~remote();

    /// \brief Initialize 'remote'
    ///
    /// \param conf is an archival configuration
    explicit remote(ss::sharded<configuration>& conf);

    /// \brief Start the remote
    ss::future<> start();

    /// \brief Stop the remote
    ///
    /// Wait until all background operations complete
    ss::future<> stop();

    /// Stop all underlying connection in the pool
    void shutdown_connections();

    /// Return max number of concurrent requests that the object
    /// can perform.
    size_t concurrency() const;

    /// \brief Download manifest from pre-defined S3 location
    ///
    /// Method downloads the manifest and handles backpressure and
    /// errors. It retries multiple times until timeout excedes.
    /// \param bucket is a bucket name
    /// \param key is an object key of the manifest
    /// \param manifest is a manifest to download
    /// \return future that returns success code
    ss::future<download_result> download_manifest(
      const s3::bucket_name& bucket,
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
      const s3::bucket_name& bucket,
      const remote_manifest_path& key,
      base_manifest& manifest,
      retry_chain_node& parent);

    /// \brief Upload manifest to the pre-defined S3 location
    ///
    /// \param bucket is a bucket name
    /// \param manifest is a manifest to upload
    /// \return future that returns success code
    ss::future<upload_result> upload_manifest(
      const s3::bucket_name& bucket,
      const base_manifest& manifest,
      retry_chain_node& parent);

    /// \brief Upload segment to S3
    ///
    /// The method uploads the segment while tolerating some errors. It can
    /// retry after some errors.
    /// \param reset_str is a functor that returns an input_stream that returns
    ///                  segment's data
    /// \param exposed_name is a segment's name in S3
    /// \param manifest is a manifest that should have the segment metadata
    ss::future<upload_result> upload_segment(
      const s3::bucket_name& bucket,
      const remote_segment_path& segment_path,
      uint64_t content_length,
      const reset_input_stream& reset_str,
      retry_chain_node& parent,
      lazy_abort_source& lazy_abort_source);

    /// \brief Download segment from S3
    ///
    /// The method downloads the segment while tolerating some errors. It can
    /// retry after an error.
    /// \param cons_str is a functor that consumes an input_stream with
    /// segment's data
    /// \param name is a segment's name in S3
    /// \param manifest is a manifest that should have the segment metadata
    ss::future<download_result> download_segment(
      const s3::bucket_name& bucket,
      const remote_segment_path& path,
      const try_consume_stream& cons_str,
      retry_chain_node& parent);

    /// Checks if the segment exists in the bucket
    ss::future<download_result> segment_exists(
      const s3::bucket_name& bucket,
      const remote_segment_path& path,
      retry_chain_node& parent);

    /// \brief Delete object from S3
    ///
    /// The method deletes the object. It can retry after some errors.
    ///
    /// \param path is a full S3 object path
    /// \param bucket is a name of the S3 bucket
    ss::future<upload_result> delete_object(
      const s3::bucket_name& bucket,
      const s3::object_key& path,
      retry_chain_node& parent);

    ss::future<download_result> do_download_manifest(
      const s3::bucket_name& bucket,
      const remote_manifest_path& key,
      base_manifest& manifest,
      retry_chain_node& parent,
      bool expect_missing = false);

    materialized_segments& materialized() { return *_materialized; }

private:
    ss::future<> propagate_credentials(cloud_roles::credentials credentials);
    s3::client_pool _pool;
    ss::gate _gate;
    ss::abort_source _as;
    remote_probe _probe;
    auth_refresh_bg_op _auth_refresh_bg_op;
    std::unique_ptr<materialized_segments> _materialized;
};

} // namespace cloud_storage
