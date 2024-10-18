/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "cloud_topics/app.h"

#include "cloud_topics/reconciler/reconciler.h"

namespace experimental::cloud_topics::reconciler {

app::app(
  seastar::sharded<cluster::partition_manager>* partition_manager,
  seastar::sharded<cloud_io::remote>* remote)
  : _reconciler(std::make_unique<reconciler>(partition_manager, remote)) {}

app::~app() = default;

seastar::future<> app::start() { return _reconciler->start(); }

seastar::future<> app::stop() { return _reconciler->stop(); }

} // namespace experimental::cloud_topics::reconciler
