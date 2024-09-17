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

#include "base/outcome.h"
#include "cloud_storage_clients/configuration.h"
#include "cloud_storage_clients/types.h"
#include "http/client.h"
#include "model/fundamental.h"

#include <seastar/core/lowres_clock.hh>

#include <chrono>
#include <vector>

namespace cloud_storage_clients {

using http_byte_range = std::pair<uint64_t, uint64_t>;

class client {
public:
    struct no_response {};

    virtual ~client() = default;

    virtual ss::future<result<client_self_configuration_output, error_outcome>>
    self_configure() = 0;

    /// Stop the client
    virtual ss::future<> stop() = 0;

    /// Shutdown the underlying connection
    virtual void shutdown() = 0;

    /// Download object from cloud storage.
    ///
    /// \param name is a bucket name
    /// \param key is an object key
    /// \param timeout is a timeout of the operation
    /// \param expect_no_such_key log missing key events as warnings if false
    /// \return future that becomes ready after request was sent
    virtual ss::future<result<http::client::response_stream_ref, error_outcome>>
    get_object(
      const bucket_name& name,
      const object_key& key,
      ss::lowres_clock::duration timeout,
      bool expect_no_such_key = false,
      std::optional<http_byte_range> byte_range = std::nullopt)
      = 0;

    struct head_object_result {
        uint64_t object_size;
        ss::sstring etag;
    };

    /// Get metadata for object from cloud storage.
    ///
    /// \param name is a bucket name
    /// \param key is an object key
    /// \param timeout is a timeout of the operation
    /// \return future that becomes ready when the request is completed
    virtual ss::future<result<head_object_result, error_outcome>> head_object(
      const bucket_name& name,
      const object_key& key,
      ss::lowres_clock::duration timeout)
      = 0;

    /// Upload object to cloud storage
    ///
    /// \param name is a bucket name
    /// \param key is an id of the object
    /// \param payload_size is a size of the object in bytes
    /// \param body is an input_stream that can be used to read body
    /// \param timeout is a timeout of the operation
    /// \param accept_no_content accepts a 204 response as valid
    /// \return future that becomes ready when the upload is completed
    virtual ss::future<result<no_response, error_outcome>> put_object(
      const bucket_name& name,
      const object_key& key,
      size_t payload_size,
      ss::input_stream<char> body,
      ss::lowres_clock::duration timeout,
      bool accept_no_content = false)
      = 0;

    struct list_bucket_item {
        ss::sstring key;
        std::chrono::system_clock::time_point last_modified;
        size_t size_bytes;
        ss::sstring etag;
    };
    struct list_bucket_result {
        bool is_truncated = false;
        ss::sstring prefix;
        ss::sstring next_continuation_token;
        std::vector<list_bucket_item> contents;
        std::vector<ss::sstring> common_prefixes;
    };

    /// A predicate to allow list_objects to collect items selectively, saving
    /// memory. This cannot be ss::noncopyable_function because remote needs to
    /// copy this object for calls in a loop.
    using item_filter = std::function<bool(const list_bucket_item&)>;

    /// List the objects in a bucket
    ///
    /// \param name is a bucket name
    /// \param prefix optional prefix of objects to list
    /// \param start_after optional object key to start listing after
    /// \param max_keys optional upper bound on the number of returned keys
    /// \param continuation_token token returned by a previous call
    /// \param timeout operation timeout
    /// \param delimiter character used to group prefixes when listing
    /// \param collect_item_if if present, only items passing this predicate are
    /// collected.
    /// \return future that becomes ready when the request is completed
    virtual ss::future<result<list_bucket_result, error_outcome>> list_objects(
      const bucket_name& name,
      std::optional<object_key> prefix = std::nullopt,
      std::optional<object_key> start_after = std::nullopt,
      std::optional<size_t> max_keys = std::nullopt,
      std::optional<ss::sstring> continuation_token = std::nullopt,
      ss::lowres_clock::duration timeout = http::default_connect_timeout,
      std::optional<char> delimiter = std::nullopt,
      std::optional<item_filter> collect_item_if = std::nullopt)
      = 0;

    /// Delete object from cloud storage
    ///
    /// \param name is a bucket name
    /// \param key is an object key
    /// \param timeout is a timeout of the operation
    /// \return future that becomes ready when the request is completed
    virtual ss::future<result<no_response, error_outcome>> delete_object(
      const bucket_name& bucket,
      const object_key& key,
      ss::lowres_clock::duration timeout)
      = 0;

    struct delete_objects_result {
        struct key_reason {
            object_key key;
            ss::sstring reason;
        };
        std::vector<key_reason> undeleted_keys{};
    };

    constexpr static auto delete_objects_max_keys = 1000;

    /// DeleteObjects request.
    ///
    /// \pre the number of keys is <= delete_objects_max_keys (as
    /// per the S3 api)
    ///
    /// \param bucket the name of the bucket
    /// \param keys the keys in the bucket to delete
    /// \param timeout request timeout
    /// \return a future of delete_objects_result. This contains
    /// a list of keys that where not deleted, each with the Error.Code returned
    /// from the server
    /// note that keys not found in the bucket are NOT reported (they are
    /// considered as successfully deleted)
    virtual ss::future<result<delete_objects_result, error_outcome>>
    delete_objects(
      const bucket_name& bucket,
      std::vector<object_key> keys,
      ss::lowres_clock::duration timeout)
      = 0;
};

} // namespace cloud_storage_clients
