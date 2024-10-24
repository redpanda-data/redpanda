/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "cloud_storage_clients/types.h"
#include "config/types.h"
#include "test_utils/test.h"

TEST(FromConfig, S3UrlStyle) {
    ASSERT_EQ(cloud_storage_clients::from_config(std::nullopt), std::nullopt);

    ASSERT_EQ(
      cloud_storage_clients::from_config(config::s3_url_style::path),
      cloud_storage_clients::s3_url_style::path);

    ASSERT_EQ(
      cloud_storage_clients::from_config(config::s3_url_style::virtual_host),
      cloud_storage_clients::s3_url_style::virtual_host);
}
