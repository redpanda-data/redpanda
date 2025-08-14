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

#include "proto/redpanda/core/admin/shadow_link.proto.h"

namespace admin {
class shadow_link_service_impl : public proto::admin::shadow_link_service {
public:
    shadow_link_service_impl() = default;

    ss::future<proto::admin::shadow_link>
      create_shadow_link(proto::admin::create_shadow_link_request) override;

    ss::future<proto::admin::delete_shadow_link_response>
      delete_shadow_link(proto::admin::delete_shadow_link_request) override;

    ss::future<proto::admin::shadow_link>
      get_shadow_link(proto::admin::get_shadow_link_request) override;

    ss::future<proto::admin::list_shadow_links_response>
      list_shadow_links(proto::admin::list_shadow_links_request) override;

    ss::future<proto::admin::shadow_link>
      update_shadow_link(proto::admin::update_shadow_link_request) override;

    ss::future<proto::admin::shadow_link>
      fail_over(proto::admin::fail_over_request) override;
};
} // namespace admin
