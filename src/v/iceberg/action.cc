// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "iceberg/action.h"

namespace iceberg {

std::ostream& operator<<(std::ostream& o, action::errc e) {
    switch (e) {
        using enum action::errc;
    case unexpected_state:
        return o << "action::errc::unexpected_state";
    case io_failed:
        return o << "action::errc::io_failed";
    case shutting_down:
        return o << "action::errc::shutting_down";
    }
}

} // namespace iceberg
