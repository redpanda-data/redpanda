// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "test_utils/scoped_config.h"

class storage_manual_mixin {
public:
    storage_manual_mixin() {
        cfg.get("log_segment_size_min")
          .set_value(std::make_optional<uint64_t>(1));
        cfg.get("log_disable_housekeeping_for_tests").set_value(true);
    }

private:
    scoped_config cfg;
};
