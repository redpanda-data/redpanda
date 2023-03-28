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

namespace kafka {

// sorted
class connection_context;
class coordinator_ntp_mapper;
class fetch_session_cache;
class group_manager;
class group_router;
class quota_manager;
class snc_quota_manager;
class request_context;
class rm_group_frontend;
class rm_group_proxy_impl;
class usage_manager;

} // namespace kafka
