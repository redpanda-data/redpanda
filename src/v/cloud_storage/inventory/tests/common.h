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

#include "cloud_storage/remote.h"

#include <gmock/gmock.h>

namespace cloud_storage::inventory {

class MockRemote : public cloud_storage::cloud_storage_api {
public:
    MOCK_METHOD(
      ss::future<cloud_storage::upload_result>,
      upload_object,
      (cloud_storage::upload_request),
      (override));
    MOCK_METHOD(
      ss::future<cloud_storage::download_result>,
      download_object,
      (cloud_storage::download_request),
      (override));
    MOCK_METHOD(
      ss::future<cloud_storage::cloud_storage_api::list_result>,
      list_objects,
      (const cloud_storage_clients::bucket_name&,
       retry_chain_node&,
       std::optional<cloud_storage_clients::object_key>,
       std::optional<char>,
       std::optional<cloud_storage_clients::client::item_filter>,
       std::optional<size_t>),
      (override));
    MOCK_METHOD(
      ss::future<cloud_storage::download_result>,
      object_exists,
      (const cloud_storage_clients::bucket_name&,
       const cloud_storage_clients::object_key&,
       retry_chain_node&,
       existence_check_type),
      (override));
};

} // namespace cloud_storage::inventory
