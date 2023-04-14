// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "cluster/fwd.h"
#include "migrations/feature_migrator.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>

namespace features::migrators {

class cloud_storage_config : public feature_migrator {
public:
    cloud_storage_config(cluster::controller& c)
      : feature_migrator(c) {}

    ~cloud_storage_config() {}

private:
    virtual ss::future<> do_mutate() override;

    features::feature _feature{features::feature::cloud_retention};
    features::feature get_feature() override { return _feature; }
};
} // namespace features::migrators
