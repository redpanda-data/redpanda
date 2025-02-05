/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "cluster/partition_probe_part.h"

namespace datalake {

class translation_probe final : public cluster::partition_probe_part {
public:
    void increment_invalid_record_action() noexcept;

protected:
    void setup_public_metrics(
      const model::ntp& ntp, metrics::public_metric_groups& group) final;

private:
    int64_t _invalid_record_action{0};
};

}; // namespace datalake
