/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "panda_link/manager.h"

#include "model/namespace.h"
#include "panda_link/logger.h"

namespace panda_link {
manager::manager(
  model::node_id self,
  std::unique_ptr<link_registry> registry,
  std::unique_ptr<link_factory> factory)
  : _self(self)
  , _queue([](const std::exception_ptr& ex) {
      vlog(pllog.warn, "unexpected panda link manager error: {}", ex);
  })
  , _registry(std::move(registry))
  , _factory(std::move(factory)) {}

ss::future<> manager::start() { return ss::now(); }

ss::future<> manager::stop() {
    vlog(pllog.info, "Stopping panda link manager");
    co_await _queue.shutdown();
    vlog(pllog.info, "Panda link manager stopped");
}

void manager::on_link_change(model::panda_link_id id) {
    _queue.submit([this, id] { return handle_link_change(id); });
}

void manager::on_leadership_change(model::ntp ntp, ntp_leader is_leader) {
    if (ntp == model::controller_ntp) {
        on_controller_leadership_change(is_leader);
    } else {
        on_ktp_leadership_change(std::move(ntp), is_leader);
    }
}

ss::future<> manager::handle_link_change(model::panda_link_id id) {
    vlog(pllog.trace, "Handling link change for id {}", id);

    auto meta = _registry->lookup_by_id(id);
    if (!meta) {
        vlog(pllog.debug, "Detected link being removed for id {}", id);
        return ss::now();
    }

    vlog(pllog.debug, "Detected change on link {}: {}", id, *meta);
    return ss::now();
}

void manager::on_controller_leadership_change(ntp_leader is_leader) {
    vlog(pllog.trace, "Detected controller leadership change: {}", is_leader);
    _is_controller_leader = is_leader;
}

void manager::on_ktp_leadership_change(model::ntp ntp, ntp_leader is_leader) {
    vlog(pllog.trace, "Detected KTP leadership change: {} {}", ntp, is_leader);
}
} // namespace panda_link
