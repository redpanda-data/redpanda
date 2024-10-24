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
#include "test_utils/scoped_config.h"

class manual_metadata_upload_mixin {
public:
    manual_metadata_upload_mixin() {
        test_local_cfg.get("enable_cluster_metadata_upload_loop")
          .set_value(false);
    }

private:
    scoped_config test_local_cfg;
};
