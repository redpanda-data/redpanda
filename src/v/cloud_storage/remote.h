/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_storage/manifest.h"
#include "cloud_storage/probe.h"
#include "cloud_storage/types.h"
#include "random/simple_time_jitter.h"
#include "s3/client.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>

namespace cloud_storage {

/// \brief Represents remote endpoint
///
/// The `remote` is responsible for remote data
/// transfer and storage. It 'knows' how to upload and
/// download data. Also, it's responsible for maintaining
/// correct naming in S3. The remote takes into account
/// things like reconnects, backpressure and backoff.
class remote {
public:
    /// Functor that returns fresh input_stream object that can be used
    /// to re-upload and will return all data that needs to be uploaded
    using reset_input_stream = std::function<ss::input_stream<char>()>;

    /// Functor that attempts to consume the input stream. If the connection
    /// is broken during the download the functor is responsible for he cleanup.
    /// The functor should be reenterable since it can be called many times.
    /// On success it should return content_length. On failure it should
    /// allow the exception from the input_stream to propagate.
    using try_consume_stream
      = std::function<ss::future<uint64_t>(uint64_t, ss::input_stream<char>)>;

    /// Functor that should be provided by user when list_objects api is called.
    /// It receives every key that matches the query as well as it's modifiation
    /// time, size in bytes, and etag.
    using list_objects_consumer = std::function<ss::stop_iteration(
      ss::sstring, std::chrono::system_clock::time_point, size_t, ss::sstring)>;

    /// \brief Initialize 'remote'
    ///
    /// \param limit is a number of simultaneous connections
    /// \param conf is an S3 configuration
    explicit remote(
      s3_connection_limit limit,
      const s3::configuration& conf,
      service_probe& probe);

    /// \brief Start the remote
    ss::future<> start();

    /// \brief Stop the remote
    ///
    /// Wait until all background operations complete
    ss::future<> stop();

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
      const segment_name& exposed_name,
      uint64_t content_length,
      const reset_input_stream& reset_str,
      manifest& manifest,
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
      const s3::bucket_name& bucket,
      const segment_name& name,
      const manifest& manifest,
      const try_consume_stream& cons_str,
      retry_chain_node& parent);

    ss::future<download_result> list_objects(
      const list_objects_consumer& cons,
      const s3::bucket_name& bucktet,
      const std::optional<s3::object_key>& prefix,
      const std::optional<size_t>& max_keys,
      retry_chain_node& parent);

private:
    s3::client_pool _pool;
    ss::gate _gate;
    ss::abort_source _as;
    service_probe& _probe;
};

} // namespace cloud_storage
