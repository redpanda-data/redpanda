/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/self_test/cloudcheck.h"

#include "base/vlog.h"
#include "cloud_storage/types.h"
#include "cluster/logger.h"
#include "cluster/self_test/metrics.h"
#include "config/configuration.h"
#include "random/generators.h"
#include "utils/uuid.h"

#include <algorithm>

namespace cluster::self_test {

cloudcheck::cloudcheck(ss::sharded<cloud_storage::remote>& cloud_storage_api)
  : _rtc(_as)
  , _cloud_storage_api(cloud_storage_api) {}

ss::future<> cloudcheck::start() { return ss::make_ready_future<>(); }

ss::future<> cloudcheck::stop() {
    _as.request_abort();
    return _gate.close();
}

void cloudcheck::cancel() { _cancelled = true; }

ss::future<std::vector<self_test_result>>
cloudcheck::run(cloudcheck_opts opts) {
    _cancelled = false;
    _opts = std::move(opts);

    if (_gate.is_closed()) {
        vlog(clusterlog.debug, "cloudcheck - gate already closed");
        auto result = self_test_result{
          .name = _opts.name,
          .test_type = "cloud",
          .start_time = time_since_epoch(ss::lowres_system_clock::now()),
          .end_time = time_since_epoch(ss::lowres_system_clock::now()),
          .warning = "cloudcheck - gate already closed"};
        co_return std::vector<self_test_result>{result};
    }
    auto g = _gate.hold();

    vlog(
      clusterlog.info,
      "Starting redpanda self-test cloud benchmark, with options: {}",
      opts);

    const auto& cfg = config::shard_local_cfg();
    if (!cfg.cloud_storage_enabled()) {
        vlog(
          clusterlog.warn,
          "Cloud storage is not enabled, exiting cloud storage self-test.");
        auto result = self_test_result{
          .name = _opts.name,
          .test_type = "cloud",
          .start_time = time_since_epoch(ss::lowres_system_clock::now()),
          .end_time = time_since_epoch(ss::lowres_system_clock::now()),
          .warning = "Cloud storage is not enabled."};
        co_return std::vector<self_test_result>{result};
    }

    if (!_cloud_storage_api.local_is_initialized()) {
        vlog(
          clusterlog.warn,
          "_cloud_storage_api is not initialized, exiting cloud storage "
          "self-test.");
        auto result = self_test_result{
          .name = _opts.name,
          .test_type = "cloud",
          .start_time = time_since_epoch(ss::lowres_system_clock::now()),
          .end_time = time_since_epoch(ss::lowres_system_clock::now()),
          .warning = "cloud_storage_api is not initialized."};
        co_return std::vector<self_test_result>{result};
    }

    co_return co_await ss::with_scheduling_group(
      _opts.sg, [this]() mutable { return run_benchmarks(); });
}

template<typename Test, typename... Args, typename R>
R cloudcheck::do_run_test(Test test, Args&&... args) {
    const auto start = ss::lowres_system_clock::now();
    auto result = co_await std::invoke(
      test, *this, std::forward<Args>(args)...);
    const auto end = ss::lowres_system_clock::now();
    result.test_result.start_time = time_since_epoch(start);
    result.test_result.end_time = time_since_epoch(end);
    result.test_result.duration = end - start;
    co_return result;
}

ss::future<std::vector<self_test_result>> cloudcheck::run_benchmarks() {
    std::vector<self_test_result> results;

    const auto bucket = cloud_storage_clients::bucket_name{
      cloud_storage::configuration::get_bucket_config()().value()};

    const auto self_test_prefix = cloud_storage_clients::object_key{
      "self-test/"};

    const auto uuid = cloud_storage_clients::object_key{
      ss::sstring{uuid_t::create()}};
    const auto self_test_key = cloud_storage_clients::object_key{
      self_test_prefix / uuid};

    const iobuf payload = make_random_payload();

    // Test Put
    auto verify_test_result = co_await do_run_test(
      &cloudcheck::verify_upload, bucket, self_test_key, payload);
    auto upload_test_result = verify_test_result.test_result;
    const bool is_uploaded
      = (!upload_test_result.warning && !upload_test_result.error);

    results.push_back(std::move(upload_test_result));

    // Test List
    // This will attempt to list from the self_test_prefix, if the payload was
    // uploaded.
    const std::optional<cloud_storage_clients::object_key> list_prefix
      = (is_uploaded) ? self_test_prefix
                      : std::optional<cloud_storage_clients::object_key>{};
    auto list_test_result_pair = co_await do_run_test(
      &cloudcheck::verify_list, bucket, list_prefix, num_default_objects);
    auto& [object_list, list_test_result] = list_test_result_pair;
    if (is_uploaded && object_list) {
        // Check that uploaded object exists in object_list contents.
        auto& object_list_contents = object_list.value().contents;
        auto payload_item_it = std::find_if(
          object_list_contents.begin(),
          object_list_contents.end(),
          [self_test_key](const auto& item) {
              return item.key == self_test_key();
          });

        if (payload_item_it == object_list_contents.end()) {
            list_test_result.error = "Uploaded key/payload could not be found "
                                     "in cloud storage item list.";
        }
    }

    results.push_back(std::move(list_test_result));

    // Test Get
    // If the payload was uploaded, this attempts to get the written
    // object. If it wasn't, it will attempt to get the smallest object from the
    // object list, if at least one exists.
    auto get_min_object_key =
      [&object_list]() -> std::optional<cloud_storage_clients::object_key> {
        if (!object_list) {
            return std::nullopt;
        }

        auto& object_list_contents = object_list.value().contents;
        if (object_list_contents.empty()) {
            return std::nullopt;
        }

        // Get the smallest file from object_list.
        auto smallest_object = *std::min_element(
          object_list_contents.begin(),
          object_list_contents.end(),
          [](const auto& a, const auto& b) {
              return a.size_bytes < b.size_bytes;
          });
        return cloud_storage_clients::object_key{smallest_object.key};
    };

    const std::optional<cloud_storage_clients::object_key> download_key
      = (is_uploaded) ? self_test_key : get_min_object_key();
    auto download_test_result_pair = co_await do_run_test(
      &cloudcheck::verify_download, bucket, download_key);
    auto& [downloaded_object, download_test_result] = download_test_result_pair;
    if (is_uploaded && downloaded_object) {
        auto& downloaded_buf = downloaded_object.value();
        if (downloaded_buf != payload) {
            download_test_result.error
              = "Downloaded object differs from uploaded payload.";
        }
    }

    results.push_back(std::move(download_test_result));

    // Test Head
    auto head_test_result = co_await do_run_test(
      &cloudcheck::verify_head, bucket, download_key);
    results.push_back(std::move(head_test_result.test_result));

    // Test Delete
    auto delete_test_result = co_await do_run_test(
      &cloudcheck::verify_delete, bucket, self_test_key);
    results.push_back(std::move(delete_test_result.test_result));

    // Test Deletes
    auto deletes_test_result = co_await do_run_test(
      &cloudcheck::verify_deletes, bucket, num_default_objects);
    results.push_back(std::move(deletes_test_result.test_result));

    co_return results;
}

iobuf cloudcheck::make_random_payload(size_t size) const {
    iobuf payload;
    const auto random_data = random_generators::gen_alphanum_string(size);
    payload.append(random_data.data(), size);
    return payload;
}

cloud_storage::upload_request cloudcheck::make_upload_request(
  const cloud_storage_clients::bucket_name& bucket,
  const cloud_storage_clients::object_key& key,
  iobuf payload,
  retry_chain_node& rtc) {
    cloud_storage::upload_request upload_request(
      {.bucket = bucket, .key = key, .parent_rtc = rtc},
      cloud_storage::upload_type::object,
      std::move(payload));
    return upload_request;
}

cloud_storage::download_request cloudcheck::make_download_request(
  const cloud_storage_clients::bucket_name& bucket,
  const cloud_storage_clients::object_key& key,
  iobuf& payload,
  retry_chain_node& rtc) {
    cloud_storage::download_request download_request(
      {.bucket = bucket, .key = key, .parent_rtc = rtc},
      cloud_storage::download_type::object,
      std::ref(payload));
    return download_request;
}

ss::future<cloudcheck::verify_upload_result> cloudcheck::verify_upload(
  cloud_storage_clients::bucket_name bucket,
  cloud_storage_clients::object_key key,
  const iobuf& payload) {
    auto result = self_test_result{
      .name = _opts.name, .info = "Put", .test_type = "cloud"};

    if (_cancelled) {
        result.warning = "Run was manually cancelled.";
        co_return result;
    }

    try {
        auto rtc = retry_chain_node(_opts.timeout, _opts.backoff, &_rtc);
        cloud_storage::upload_request upload_request = make_upload_request(
          bucket, key, payload.copy(), rtc);
        const cloud_storage::upload_result upload_result
          = co_await _cloud_storage_api.local().upload_object(
            std::move(upload_request));

        switch (upload_result) {
        case cloud_storage::upload_result::success:
            break;
        case cloud_storage::upload_result::timedout:
            [[fallthrough]];
        case cloud_storage::upload_result::failed:
            [[fallthrough]];
        case cloud_storage::upload_result::cancelled:
            result.error = "Failed to upload to cloud storage.";
            break;
        }
    } catch (const std::exception& e) {
        result.error = e.what();
    }

    co_return result;
}

ss::future<cloudcheck::verify_list_result> cloudcheck::verify_list(
  cloud_storage_clients::bucket_name bucket,
  std::optional<cloud_storage_clients::object_key> prefix,
  size_t max_keys) {
    auto result = self_test_result{
      .name = _opts.name, .info = "List", .test_type = "cloud"};

    if (_cancelled) {
        result.warning = "Run was manually cancelled.";
        co_return verify_list_result{
          cloud_storage_clients::error_outcome::fail, result};
    }

    try {
        auto rtc = retry_chain_node(_opts.timeout, _opts.backoff, &_rtc);
        const cloud_storage::remote::list_result object_list
          = co_await _cloud_storage_api.local().list_objects(
            bucket, rtc, prefix, std::nullopt, std::nullopt, max_keys);

        if (!object_list) {
            result.error = "Failed to list objects in cloud storage.";
        }

        co_return verify_list_result{std::move(object_list), std::move(result)};
    } catch (const std::exception& e) {
        result.error = e.what();
    }

    co_return verify_list_result{
      cloud_storage_clients::error_outcome::fail, std::move(result)};
}

ss::future<cloudcheck::verify_head_result> cloudcheck::verify_head(
  cloud_storage_clients::bucket_name bucket,
  std::optional<cloud_storage_clients::object_key> key) {
    auto result = self_test_result{
      .name = _opts.name, .info = "Head", .test_type = "cloud"};

    if (_cancelled) {
        result.warning = "Run was manually cancelled.";
        co_return result;
    }

    if (!key) {
        result.warning = "Could not download from cloud storage (no file was "
                         "found in the bucket).";
        co_return result;
    }

    try {
        auto rtc = retry_chain_node(_opts.timeout, _opts.backoff, &_rtc);
        const cloud_storage::download_result head_result
          = co_await _cloud_storage_api.local().object_exists(
            bucket,
            key.value(),
            rtc,
            cloud_storage::existence_check_type::object);

        switch (head_result) {
        case cloud_storage::download_result::success:
            break;
        case cloud_storage::download_result::timedout:
            [[fallthrough]];
        case cloud_storage::download_result::failed:
            [[fallthrough]];
        case cloud_storage::download_result::notfound:
            result.error = "Failed to download from cloud storage.";
            break;
        }

    } catch (const std::exception& e) {
        result.error = e.what();
    }

    co_return result;
}

ss::future<cloudcheck::verify_download_result> cloudcheck::verify_download(
  cloud_storage_clients::bucket_name bucket,
  std::optional<cloud_storage_clients::object_key> key) {
    auto result = self_test_result{
      .name = _opts.name, .info = "Get", .test_type = "cloud"};

    if (_cancelled) {
        result.warning = "Run was manually cancelled.";
        co_return verify_download_result{std::nullopt, result};
    }

    if (!key) {
        result.warning = "Could not download from cloud storage (no file was "
                         "found in the bucket).";
        co_return verify_download_result{std::nullopt, result};
    }

    std::optional<iobuf> result_payload = std::nullopt;

    try {
        iobuf download_payload;
        auto rtc = retry_chain_node(_opts.timeout, _opts.backoff, &_rtc);
        cloud_storage::download_request download_request
          = make_download_request(
            bucket, key.value(), std::ref(download_payload), rtc);

        const cloud_storage::download_result download_result
          = co_await _cloud_storage_api.local().download_object(
            std::move(download_request));

        switch (download_result) {
        case cloud_storage::download_result::success:
            result_payload = std::move(download_payload);
            break;
        case cloud_storage::download_result::timedout:
            [[fallthrough]];
        case cloud_storage::download_result::failed:
            [[fallthrough]];
        case cloud_storage::download_result::notfound:
            result.error = "Failed to download from cloud storage.";
            break;
        }
    } catch (const std::exception& e) {
        result.error = e.what();
    }

    co_return verify_download_result{
      std::move(result_payload), std::move(result)};
}

ss::future<cloudcheck::verify_delete_result> cloudcheck::verify_delete(
  cloud_storage_clients::bucket_name bucket,
  cloud_storage_clients::object_key key) {
    auto result = self_test_result{
      .name = _opts.name, .info = "Delete", .test_type = "cloud"};

    if (_cancelled) {
        result.warning = "Run was manually cancelled.";
        co_return result;
    }

    try {
        auto rtc = retry_chain_node(_opts.timeout, _opts.backoff, &_rtc);
        const cloud_storage::upload_result delete_result
          = co_await _cloud_storage_api.local().delete_object(bucket, key, rtc);

        switch (delete_result) {
        case cloud_storage::upload_result::success:
            break;
        case cloud_storage::upload_result::timedout:
            [[fallthrough]];
        case cloud_storage::upload_result::failed:
            [[fallthrough]];
        case cloud_storage::upload_result::cancelled:
            result.error = "Failed to delete from cloud storage.";
            break;
        }
    } catch (const std::exception& e) {
        result.error = e.what();
    }

    co_return result;
}

ss::future<cloudcheck::verify_deletes_result> cloudcheck::verify_deletes(
  cloud_storage_clients::bucket_name bucket, size_t num_objects) {
    auto result = self_test_result{
      .name = _opts.name, .info = "Plural Delete", .test_type = "cloud"};

    if (_cancelled) {
        result.warning = "Run was manually cancelled.";
        co_return result;
    }

    std::vector<cloud_storage_clients::object_key> keys(num_objects);
    std::generate(keys.begin(), keys.end(), []() {
        return cloud_storage_clients::object_key{ss::sstring{uuid_t::create()}};
    });

    for (const auto& key : keys) {
        co_await verify_upload(bucket, key, make_random_payload());
    }

    try {
        auto rtc = retry_chain_node(_opts.timeout, _opts.backoff, &_rtc);
        const cloud_storage::upload_result delete_result
          = co_await _cloud_storage_api.local().delete_objects(
            bucket, keys, rtc);

        switch (delete_result) {
        case cloud_storage::upload_result::success:
            break;
        case cloud_storage::upload_result::timedout:
            [[fallthrough]];
        case cloud_storage::upload_result::failed:
            [[fallthrough]];
        case cloud_storage::upload_result::cancelled:
            result.error = "Failed to delete from cloud storage.";
            break;
        }
    } catch (const std::exception& e) {
        result.error = e.what();
    }

    co_return result;
}

} // namespace cluster::self_test
