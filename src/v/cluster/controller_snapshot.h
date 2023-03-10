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

#include "cluster/types.h"
#include "serde/envelope.h"
#include "serde/serde.h"

namespace cluster {

struct controller_snapshot
  : public serde::checksum_envelope<
      controller_snapshot,
      serde::version<0>,
      serde::compat_version<0>> {
    friend bool
    operator==(const controller_snapshot&, const controller_snapshot&)
      = default;

    auto serde_fields() { return std::tie(); }
};

} // namespace cluster
