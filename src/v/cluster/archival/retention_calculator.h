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

#include "cloud_storage/partition_manifest.h"
#include "model/fundamental.h"
#include "storage/log.h"
#include "storage/ntp_config.h"

namespace archival {

class retention_strategy {
public:
    virtual bool done(const cloud_storage::partition_manifest::segment_meta&)
      = 0;

    virtual ss::sstring name() const = 0;

    virtual ~retention_strategy() = default;
};

/*
 * Helper class for computing the next start offset for
 * the cloud log according to the retention policies of
 * the partition.
 */
class retention_calculator {
public:
    static std::optional<retention_calculator> factory(
      const cloud_storage::partition_manifest&, const storage::ntp_config&);

    std::optional<model::offset> next_start_offset();

    std::optional<ss::sstring> strategy_name() const;

private:
    retention_calculator(
      const cloud_storage::partition_manifest&,
      std::vector<std::unique_ptr<retention_strategy>>);

    const cloud_storage::partition_manifest& _manifest;
    std::vector<std::unique_ptr<retention_strategy>> _strategies;
};

} // namespace archival
