/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cloud_storage/manifest.h"
#include "cluster/persisted_stm.h"
#include "model/fundamental.h"
#include "model/timestamp_serde.h"
#include "raft/log_eviction_stm.h"
#include "serde/serde.h"
#include "utils/mutex.h"
#include "utils/prefix_logger.h"

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/log.hh>

#include <absl/container/btree_set.h>

#include <compare>

namespace cluster {

class archival_metadata_stm final : public persisted_stm {
public:
    struct segment : public serde::envelope<segment, serde::version<0>> {
        // ntp revision id is needed to reconstruct full remote path of
        // the segment.
        model::revision_id ntp_revision;
        cloud_storage::segment_name name;
        cloud_storage::manifest::segment_meta meta;
    };

    explicit archival_metadata_stm(raft::consensus*);

    void set_log_eviction_stm(
      ss::lw_shared_ptr<raft::log_eviction_stm> log_eviction_stm) {
        _log_eviction_stm = std::move(log_eviction_stm);
    }

    ss::future<bool> add_segments(const cloud_storage::manifest&);

    model::offset start_offset() const { return _start_offset; }

    model::offset last_offset() { return _last_offset; }

    const cloud_storage::manifest& manifest() const { return _manifest; }

private:
    std::vector<segment>
    segments_from_manifest(const cloud_storage::manifest&) const;

    ss::future<bool> do_add_segments(const cloud_storage::manifest&);

    ss::future<> apply(model::record_batch batch) override;
    ss::future<> apply_snapshot(stm_snapshot_header, iobuf&&) override;
    ss::future<> handle_eviction() override;

    ss::future<stm_snapshot> take_snapshot() override;

    void apply_add_segment(const segment& segment);

private:
    prefix_logger _logger;

    mutex _lock;

    cloud_storage::manifest _manifest;
    model::offset _start_offset;
    model::offset _last_offset;

    ss::lw_shared_ptr<raft::log_eviction_stm> _log_eviction_stm;
};

} // namespace cluster
