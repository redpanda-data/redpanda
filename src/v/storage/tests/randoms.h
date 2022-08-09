/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "model/tests/randoms.h"
#include "random/generators.h"
#include "storage/types.h"
#include "test_utils/randoms.h"

namespace storage {

inline storage::disk random_disk() {
    return storage::disk{
      {},
      random_generators::gen_alphanum_string(20),
      random_generators::get_int<uint64_t>(),
      random_generators::get_int<uint64_t>()};
}

inline disk_space_alert random_disk_space_alert() {
    return disk_space_alert(random_generators::get_int(
      static_cast<int>(disk_space_alert::ok),
      static_cast<int>(disk_space_alert::degraded)));
}

} // namespace storage
