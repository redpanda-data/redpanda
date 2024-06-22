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

#include "cluster/archival/ntp_archiver_service.h"
#include "cluster/archival/types.h"
#include "config/property.h"
#include "utils/retry_chain_node.h"

#include <optional>

namespace archival {

/// Re-uploads small segments to S3
class adjacent_segment_merger : public housekeeping_job {
public:
    explicit adjacent_segment_merger(
      ntp_archiver& parent, bool, config::binding<bool>);

    ss::future<run_result> run(run_quota_t quota) override;

    void interrupt() override;

    bool interrupted() const override;

    ss::future<> stop() override;

    void set_enabled(bool) override;

    void acquire() override;
    void release() override;

    retry_chain_node* get_root_retry_chain_node() override;

    ss::sstring name() const override;

private:
    std::optional<adjacent_segment_run> scan_manifest(
      model::offset local_start_offset,
      const cloud_storage::partition_manifest& manifest);

    const bool _is_local;

    bool enabled() { return _config_enabled() && _job_enabled; }

    // Whether segment merging is enabled in the cluster config (i.e. by
    // the administrator)
    config::binding<bool> _config_enabled;

    // Whether segment merging is enabled at the housekeeping job level (e.g.
    // may be disabled when not leader)
    bool _job_enabled{true};

    model::offset _last;
    ntp_archiver& _archiver;
    config::binding<std::optional<size_t>> _target_segment_size;
    config::binding<std::optional<size_t>> _min_segment_size;
    ss::abort_source _as;
    retry_chain_node _root_rtc;
    retry_chain_logger _ctxlog;
    ss::gate _gate;
    ss::gate::holder _holder;
};

} // namespace archival
