/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "security/audit/audit_log_manager.h"
#include "security/audit/schemas/types.h"

namespace security::audit::test {

user make_random_user();
authentication_event_options make_random_authn_options();
ss::future<size_t> pending_audit_events(audit_log_manager& m);
ss::future<> set_auditing_config_options(size_t event_size);

} // namespace security::audit::test
