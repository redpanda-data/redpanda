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
#include "features/feature_table_snapshot.h"
#include "serde/envelope.h"
#include "serde/serde.h"

namespace cluster {

namespace controller_snapshot_parts {

struct bootstrap_t
  : public serde::
      envelope<bootstrap_t, serde::version<0>, serde::compat_version<0>> {
    std::optional<model::cluster_uuid> cluster_uuid;

    friend bool operator==(const bootstrap_t&, const bootstrap_t&) = default;

    auto serde_fields() { return std::tie(cluster_uuid); }
};

struct features_t
  : public serde::
      envelope<features_t, serde::version<0>, serde::compat_version<0>> {
    features::feature_table_snapshot snap;

    friend bool operator==(const features_t&, const features_t&) = default;

    auto serde_fields() { return std::tie(snap); }
};

} // namespace controller_snapshot_parts

struct controller_snapshot
  : public serde::checksum_envelope<
      controller_snapshot,
      serde::version<0>,
      serde::compat_version<0>> {
    controller_snapshot_parts::bootstrap_t bootstrap;
    controller_snapshot_parts::features_t features;

    friend bool
    operator==(const controller_snapshot&, const controller_snapshot&)
      = default;

    ss::future<> serde_async_write(iobuf&);
    ss::future<> serde_async_read(iobuf_parser&, serde::header const);
};

} // namespace cluster
