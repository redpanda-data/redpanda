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

#pragma once

#include "model/fundamental.h"
#include "model/panda_link.h"
#include "ssx/work_queue.h"

namespace panda_link {

using ntp_leader = ss::bool_class<struct is_ntp_leader_tag>;
/**
 * @brief Abstract class used to access registered panda links
 */
class link_registry {
public:
    link_registry() = default;
    link_registry(const link_registry&) = delete;
    link_registry& operator=(const link_registry&) = delete;
    link_registry(link_registry&&) = delete;
    link_registry& operator=(link_registry&&) = delete;
    virtual ~link_registry() = default;

    virtual std::optional<model::panda_link_metadata>
      lookup_by_id(model::panda_link_id) const = 0;
};

/**
 * @brief Abstract class used to create links
 */
class link_factory {
public:
    link_factory() = default;
    link_factory(const link_factory&) = delete;
    link_factory& operator=(const link_factory&) = delete;
    link_factory(link_factory&&) = delete;
    link_factory& operator=(link_factory&&) = delete;
    virtual ~link_factory() = default;
};

class manager {
public:
    manager(
      model::node_id,
      std::unique_ptr<link_registry>,
      std::unique_ptr<link_factory>);
    manager(const manager&) = delete;
    manager& operator=(const manager&) = delete;
    manager(manager&&) = delete;
    manager& operator=(manager&&) = delete;
    virtual ~manager() = default;

    ss::future<> start();
    ss::future<> stop();

    void on_link_change(model::panda_link_id);
    void on_leadership_change(model::ntp, ntp_leader);

private:
    ss::future<> handle_link_change(model::panda_link_id);

    void on_controller_leadership_change(ntp_leader);

    void on_ktp_leadership_change(model::ntp, ntp_leader);

private:
    model::node_id _self;
    ssx::work_queue _queue;
    std::unique_ptr<link_registry> _registry;
    std::unique_ptr<link_factory> _factory;
    ntp_leader _is_controller_leader{ntp_leader::no};
};

} // namespace panda_link
