/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "security/audit/tests/audit_test_utils.h"

#include "config/configuration.h"
#include "random/generators.h"

namespace security::audit::test {
user make_random_user() {
    return user{
      .domain = random_generators::gen_alphanum_string(10),
      .name = random_generators::gen_alphanum_string(10),
      .type_id = random_generators::random_choice(
        {user::type::user,
         user::type::admin,
         user::type::system,
         user::type::other,
         user::type::unknown}),
      .uid = random_generators::gen_alphanum_string(10),
    };
}

authentication_event_options make_random_authn_options() {
    return authentication_event_options{
      .server_addr = net::unresolved_address(
        "1.2.3.4", random_generators::get_int(9999)),
      .client_addr = net::unresolved_address(
        "1.2.3.4", random_generators::get_int(9999)),
      .is_cleartext = authentication::used_cleartext(
        random_generators::get_int(0, 1)),
      .user = make_random_user(),
      .error_reason = std::make_optional(
        random_generators::gen_alphanum_string(10))};
}

ss::future<size_t> pending_audit_events(audit_log_manager& m) {
    return m.container().map_reduce0(
      [](const audit_log_manager& m) { return m.pending_events(); },
      size_t(0),
      std::plus<>());
}

ss::future<> set_auditing_config_options(size_t event_size) {
    return ss::smp::invoke_on_all([event_size] {
        std::vector<ss::sstring> enabled_types{"authenticate"};
        config::shard_local_cfg().get("audit_enabled").set_value(false);
        config::shard_local_cfg()
          .get("audit_log_replication_factor")
          .set_value(std::make_optional(int16_t(1)));
        config::shard_local_cfg()
          .get("audit_queue_max_buffer_size_per_shard")
          .set_value(event_size * 100);
        config::shard_local_cfg()
          .get("audit_queue_drain_interval_ms")
          .set_value(std::chrono::milliseconds(60000));
        config::shard_local_cfg()
          .get("audit_enabled_event_types")
          .set_value(enabled_types);
    });
}
} // namespace security::audit::test
