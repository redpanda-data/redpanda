// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/vlog.h"
#include "bytes/bytes.h"
#include "bytes/iostream.h"
#include "cloud_io/remote.h"
#include "cloud_storage_clients/types.h"
#include "model/timestamp.h"
#include "random/generators.h"

#include <seastar/core/file-types.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>

#include <chrono>
#include <stdexcept>

using namespace std::chrono_literals;

// This header contains hand written mocks for the cloud_storage api's (cache
// and remote)

template<class Clock = seastar::manual_clock>
class remote_mock final : public cloud_io::remote_api<Clock> {
public:
    using reset_input_stream = cloud_io::remote_api<Clock>::reset_input_stream;

    ss::future<cloud_io::download_result>
    download_object(cloud_io::basic_download_request<Clock> req) override {
        auto path = req.transfer_details.key();
        // Read what was previously written into the remote_mock
        auto it = _requests.find(path);
        if (it == _requests.end()) {
            co_return cloud_io::download_result::notfound;
        }
        req.payload = it->second.copy();
        co_return cloud_io::download_result::success;
    }

    ss::future<cloud_io::download_result> object_exists(
      const cloud_storage_clients::bucket_name&,
      const cloud_storage_clients::object_key&,
      basic_retry_chain_node<Clock>&,
      std::string_view) override {
        throw std::runtime_error("Not implemented");
    }

    /// \brief Upload small objects to bucket. Suitable for uploading simple
    /// strings, does not check for leadership before upload like the segment
    /// upload function.
    ss::future<cloud_io::upload_result>
    upload_object(cloud_io::basic_upload_request<Clock> req) override {
        auto jitter = random_generators::get_int(_max_time_jitter.count());
        co_await seastar::sleep<Clock>(std::chrono::milliseconds(jitter));

        if (_next_upload_result == cloud_io::upload_result::success) {
            _requests.insert(std::make_pair(
              req.transfer_details.key(), std::move(req.payload)));
        } else {
            _failed_requests.insert(std::make_pair(
              req.transfer_details.key(), std::move(req.payload)));
        }
        co_return _next_upload_result;
    }

    ss::future<cloud_io::upload_result> upload_stream(
      cloud_io::basic_transfer_details<Clock> transfer_details,
      uint64_t content_length,
      const reset_input_stream& reset_str,
      lazy_abort_source& lazy_abort_source,
      const std::string_view stream_label,
      std::optional<size_t> max_retries) override {
        auto provider = co_await reset_str();
        auto str = provider->take_stream();
        co_await provider->close();
        iobuf data;
        auto out_str = make_iobuf_ref_output_stream(data);
        co_await ss::copy(str, out_str);
        vassert(
          data.size_bytes() == content_length,
          "Expected content_length {}, actual {}",
          content_length,
          data.size_bytes());

        // add time jitter to simulate cloud storage latency
        auto jitter = random_generators::get_int(_max_time_jitter.count());
        co_await seastar::sleep<Clock>(std::chrono::milliseconds(jitter));

        if (_next_upload_result == cloud_io::upload_result::success) {
            _requests.insert(
              std::make_pair(transfer_details.key(), std::move(data)));
        } else {
            _failed_requests.insert(
              std::make_pair(transfer_details.key(), std::move(data)));
        }
        co_return _next_upload_result;
    }

    ss::future<cloud_io::download_result> download_stream(
      cloud_io::basic_transfer_details<Clock>,
      const cloud_io::try_consume_stream&,
      const std::string_view,
      bool,
      std::optional<cloud_storage_clients::http_byte_range>,
      std::function<void(size_t)>) override {
        throw std::runtime_error("Not implemented");
    }

    void set_next_upload_result(cloud_io::upload_result r) {
        _next_upload_result = r;
    }

    // check if the segment was uploaded (includes both successful and failed
    // requests)
    bool contains(const cloud_storage_clients::object_key& sp) {
        return _requests.contains(sp()) || _failed_requests.contains(sp);
    }

    // check if the segment was uploaded (includes only successful requests)
    bool contains_successful(const cloud_storage_clients::object_key& sp) {
        return _requests.contains(sp());
    }

    std::map<std::filesystem::path, iobuf>& get_successful_requests() {
        return _requests;
    }

    std::map<std::filesystem::path, iobuf>& get_failed_requests() {
        return _failed_requests;
    }

private:
    std::chrono::milliseconds _max_time_jitter{100ms};
    cloud_io::upload_result _next_upload_result{
      cloud_io::upload_result::success};
    std::map<std::filesystem::path, iobuf> _requests;
    std::map<std::filesystem::path, iobuf> _failed_requests;
};
