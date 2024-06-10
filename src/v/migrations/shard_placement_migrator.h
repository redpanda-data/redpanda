// Copyright 2024 Redpanda Data, Inc.
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

namespace features::migrators {

class shard_placement_migrator : public feature_migrator {
public:
    shard_placement_migrator(cluster::controller& c)
      : feature_migrator(c) {}

private:
    features::feature get_feature() override { return _feature; }
    ss::future<> do_migrate() override;

    features::feature _feature{features::feature::node_local_core_assignment};
};

} // namespace features::migrators
