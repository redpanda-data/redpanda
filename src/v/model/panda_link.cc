/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "model/panda_link.h"

auto fmt::formatter<model::panda_link_connection>::format(
  const model::panda_link_connection& c,
  fmt::format_context& ctx) -> decltype(ctx.out()) {
    fmt::format_to(
      ctx.out(), "{{source_cluster_addrs: {} }}", c.source_cluster_addrs);
    return ctx.out();
}

auto fmt::formatter<model::panda_link_metadata>::format(
  const model::panda_link_metadata& m,
  fmt::format_context& ctx) -> decltype(ctx.out()) {
    fmt::format_to(
      ctx.out(), "{{name: {}, connection: {}}}", m.name, m.connection);
    return ctx.out();
}
