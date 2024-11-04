// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "cloud_topics/dl_overlay.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "serde/envelope.h"

namespace experimental::cloud_topics {

struct push_overlay_cmd
  : public serde::
      envelope<push_overlay_cmd, serde::version<0>, serde::compat_version<0>> {
    push_overlay_cmd() = default;
    explicit push_overlay_cmd(dl_overlay overlay)
      : overlay(std::move(overlay)) {}

    auto serde_fields() { return std::tie(overlay); }

    dl_overlay overlay;
};

} // namespace experimental::cloud_topics
