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

public:
    /// Attempt to add a new overlay.
    ss::future<result<bool, dl_stm_api_errc>> push_overlay(dl_overlay overlay);

    /// Find an overlay that contains the given offset. If no overlay
    /// contains the offset, find the overlay covering the next closest
    /// available offset.
    std::optional<dl_overlay> lower_bound(kafka::offset offset) const;

private:
    ss::logger& _logger;

    /// The API can only read the state of the stm. The state can be mutated
    /// only via \ref consensus::replicate calls.
    ss::shared_ptr<const dl_stm> _stm;
};

} // namespace experimental::cloud_topics
