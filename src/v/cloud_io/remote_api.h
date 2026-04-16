/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_io/io_result.h"
#include "cloud_io/transfer_details.h"
#include "cloud_storage_clients/client.h"
#include "utils/lazy_abort_source.h"
#include "utils/stream_provider.h"

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
      std::string_view object_type) = 0;

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
      std::optional<size_t> max_retries) = 0;

    virtual ss::future<download_result> download_stream(
      transfer_details transfer_details,
      const try_consume_stream& cons_str,
      const std::string_view stream_label,
      bool acquire_hydration_units,
      std::optional<cloud_storage_clients::http_byte_range> byte_range
      = std::nullopt,
      std::function<void(size_t)> throttle_metric_ms_cb = {}) = 0;
};

} // namespace cloud_io
