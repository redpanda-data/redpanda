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

namespace experimental::cloud_topics {

app::app(ss::shared_ptr<api> ptr)
  : _impl(std::move(ptr)) {}

seastar::future<> app::start() { return _impl->start(); }

seastar::future<> app::stop() { return _impl->stop(); }

ss::shared_ptr<api> app::get_api() { return _impl; }

} // namespace experimental::cloud_topics
