// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "base/outcome.h"
#include "cloud_topics/dl_overlay.h"
#include "cloud_topics/dl_snapshot.h"
#include "cloud_topics/dl_version.h"
#include "model/record.h"

#include <seastar/core/gate.hh>
#include <seastar/util/log.hh>

#include <ostream>

namespace experimental::cloud_topics {

class dl_stm;

enum class dl_stm_api_errc {
    timeout,
    not_leader,
};

std::ostream& operator<<(std::ostream& o, dl_stm_api_errc errc);

class dl_stm_api {
public:
    dl_stm_api(ss::logger& logger, ss::shared_ptr<dl_stm> stm);
    dl_stm_api(dl_stm_api&&) noexcept = default;

    ~dl_stm_api() {
        vassert(_gate.is_closed(), "object destroyed before calling stop()");
    }

public:
    ss::future<> stop();

public:
    /// Attempt to add a new overlay.
    ss::future<result<bool, dl_stm_api_errc>> push_overlay(dl_overlay overlay);

    /// Find an overlay that contains the given offset. If no overlay
    /// contains the offset, find the overlay covering the next closest
    /// available offset.
    std::optional<dl_overlay> lower_bound(kafka::offset offset) const;

    /// Request a new snapshot to be created.
    ss::future<checked<dl_snapshot_id, dl_stm_api_errc>> start_snapshot();

    /// Read the payload of a snapshot.
    std::optional<dl_snapshot_payload> read_snapshot(dl_snapshot_id id);

    /// Remove all snapshots with version less than the given version.
    /// This must be called periodically as new snapshots are being created
    /// to avoid the state growing indefinitely.
    ss::future<checked<void, dl_stm_api_errc>>
    remove_snapshots_before(dl_version last_version_to_keep);

private:
    /// Replicate a record batch and wait for it to be applied to the dl_stm.
    /// Returns the offset at which the batch was applied.
    ss::future<checked<model::offset, dl_stm_api_errc>>
    replicated_apply(model::record_batch&& batch);

private:
    ss::logger& _logger;

    /// Gate held by async operations to ensure that the API is not destroyed
    /// while an operation is in progress.
    ss::gate _gate;

    /// The API can only read the state of the stm. The state can be mutated
    /// only via \ref consensus::replicate calls.
    ss::shared_ptr<const dl_stm> _stm;
};

} // namespace experimental::cloud_topics
