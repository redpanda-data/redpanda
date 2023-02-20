/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "archival/ntp_archiver_service.h"
#include "archival/types.h"
#include "cloud_storage/remote.h"
#include "cluster/partition.h"
#include "config/bounded_property.h"
#include "config/property.h"
#include "utils/retry_chain_node.h"

#include <optional>

namespace archival {

/// Re-uploads small segments to S3
class adjacent_segment_merger : public housekeeping_job {
public:
    explicit adjacent_segment_merger(
      ntp_archiver& parent, retry_chain_logger& ctxlog, bool is_local);

    ss::future<run_result>
    run(retry_chain_node& rtc, run_quota_t quota) override;

    void interrupt() override;

    bool interrupted() const override;

    ss::future<> stop() override;

    void set_enabled(bool) override;

    void acquire() override;
    void release() override;

private:
    std::optional<adjacent_segment_run> scan_manifest(
      model::offset local_start_offset,
      const cloud_storage::partition_manifest& manifest);

    const bool _is_local;
    bool _enabled{true};
    model::offset _last;
    ntp_archiver& _archiver;
    retry_chain_logger& _ctxlog;
    config::binding<std::optional<size_t>> _target_segment_size;
    config::binding<std::optional<size_t>> _min_segment_size;
    ss::abort_source _as;
    ss::gate _gate;
    ss::gate::holder _holder;
};

} // namespace archival