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

#include "cloud_io/remote.h"
#include "cloud_io/tests/scoped_remote.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "iceberg/filesystem_catalog.h"
#include "schema/tests/fake_registry.h"

namespace datalake::tests {

// Test fixture that contains a functioning schema::registry and
// iceberg::catalog. This can be used to exercise schema resolution without
// heavier weight dependencies (e.g. REST catalog, PandaProxy).
class catalog_and_registry_fixture : public s3_imposter_fixture {
public:
    static constexpr std::string_view base_location{"test"};
    catalog_and_registry_fixture()
      : scoped_remote(cloud_io::scoped_remote::create(10, conf))
      , catalog(
          scoped_remote->remote.local(),
          bucket_name,
          ss::sstring(base_location)) {
        set_expectations_and_listen({});
    }
    std::unique_ptr<cloud_io::scoped_remote> scoped_remote;
    schema::fake_registry registry;
    iceberg::filesystem_catalog catalog;
};
} // namespace datalake::tests
