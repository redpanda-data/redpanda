/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/errc.h"
#include "model/timeout_clock.h"
#include "model/transform.h"
#include "serde/rw/envelope.h"

#include <chrono>

namespace cluster {

/**
 * Create/update a (Wasm) plugin.
 */
struct upsert_plugin_request
  : serde::envelope<
      upsert_plugin_request,
      serde::version<0>,
      serde::compat_version<0>> {
    model::transform_metadata transform;
    model::timeout_clock::duration timeout{};

    friend bool
    operator==(const upsert_plugin_request&, const upsert_plugin_request&)
      = default;

    auto serde_fields() { return std::tie(transform, timeout); }
};
struct upsert_plugin_response
  : serde::envelope<
      upsert_plugin_response,
      serde::version<0>,
      serde::compat_version<0>> {
    errc ec;

    friend bool
    operator==(const upsert_plugin_response&, const upsert_plugin_response&)
      = default;

    auto serde_fields() { return std::tie(ec); }
};

/**
 * Remove a (Wasm) plugin.
 */
struct remove_plugin_request
  : serde::envelope<
      remove_plugin_request,
      serde::version<0>,
      serde::compat_version<0>> {
    model::transform_name name;
    model::timeout_clock::duration timeout{};

    friend bool
    operator==(const remove_plugin_request&, const remove_plugin_request&)
      = default;

    auto serde_fields() { return std::tie(name, timeout); }
};
struct remove_plugin_response
  : serde::envelope<
      remove_plugin_response,
      serde::version<0>,
      serde::compat_version<0>> {
    uuid_t uuid;
    errc ec;

    friend bool
    operator==(const remove_plugin_response&, const remove_plugin_response&)
      = default;

    auto serde_fields() { return std::tie(uuid, ec); }
};

} // namespace cluster
