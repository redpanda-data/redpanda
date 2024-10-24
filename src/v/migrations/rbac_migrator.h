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

#include "cluster/fwd.h"
#include "migrations/feature_migrator.h"

namespace features::migrators {

class rbac_migrator final : public feature_migrator {
public:
    rbac_migrator(cluster::controller& c)
      : feature_migrator(c) {}

    ~rbac_migrator() noexcept = default;

private:
    ss::future<> do_mutate() override;
    features::feature get_feature() override { return _feature; }

    features::feature _feature{features::feature::role_based_access_control};
};
} // namespace features::migrators
