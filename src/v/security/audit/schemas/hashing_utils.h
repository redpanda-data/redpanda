/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "security/audit/schemas/types.h"
#include "security/authorizer.h"

namespace security::audit {

size_t api_activity_event_base_hash(
  std::string_view operation_name,
  const security::auth_result& auth_result,
  const ss::socket_address& local_address,
  std::string_view service_name,
  ss::net::inet_address client_addr);

} // namespace security::audit
