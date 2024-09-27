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
#include "base/seastarx.h"
#include "iceberg/table_requirement.h"
#include "iceberg/table_update.h"

namespace iceberg {

struct updates_and_reqs {
    chunked_vector<table_update::update> updates;
    chunked_vector<table_requirement::requirement> requirements;
};

class action {
public:
    enum class errc {
        // An invariant has been broken with some state, e.g. some ID was
        // missing that we expected to exist. May indicate an issue with
        // persisted metadata, or with uncommitted transaction state.
        unexpected_state,

        // IO failed while perfoming the action.
        // TODO: worth distinguishing from corruption?
        io_failed,

        // We're shutting down.
        shutting_down,
    };
    using action_outcome = checked<updates_and_reqs, errc>;
    // Constructs the updates and requirements needed to perform the given
    // action to the table metadata. Expected to be called once only.
    virtual ss::future<action_outcome> build_updates() && = 0;

    virtual ~action() = default;
};
std::ostream& operator<<(std::ostream& o, action::errc e);

} // namespace iceberg
