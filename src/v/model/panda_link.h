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

#include "base/seastarx.h"
#include "container/fragmented_vector.h"
#include "serde/envelope.h"
#include "utils/named_type.h"
#include "utils/unresolved_address.h"
#include "utils/uuid.h"

#include <seastar/core/sstring.hh>

namespace model {
/// ID of the panda link
using panda_link_id = named_type<int64_t, struct panda_link_id_tag>;
/// Name of the panda link
using panda_link_name = named_type<ss::sstring, struct panda_link_name_tag>;

/**
 * @brief Defines the settings for the connection to the source cluster
 *
 */
struct panda_link_connection
  : serde::envelope<
      panda_link_connection,
      serde::version<0>,
      serde::compat_version<0>> {
    /// List of unresolved addresses to use to bootstrap connection
    std::vector<net::unresolved_address> source_cluster_addrs;

    friend bool
    operator==(const panda_link_connection&, const panda_link_connection&)
      = default;

    auto serde_fields() { return std::tie(source_cluster_addrs); }
};

struct panda_link_metadata
  : serde::envelope<
      panda_link_metadata,
      serde::version<0>,
      serde::compat_version<0>> {
    /// Name of the panda link
    panda_link_name name;
    /// Unique ID of the panda link
    uuid_t uuid;
    /// Connection settings to the source cluster
    panda_link_connection connection;

    friend bool
    operator==(const panda_link_metadata&, const panda_link_metadata&)
      = default;

    auto serde_fields() { return std::tie(name, connection); }
};
} // namespace model

template<>
struct fmt::formatter<model::panda_link_connection>
  : fmt::formatter<string_view> {
    auto format(const model::panda_link_connection& c, format_context& ctx)
      -> decltype(ctx.out());
};

template<>
struct fmt::formatter<model::panda_link_metadata>
  : fmt::formatter<string_view> {
    auto format(const model::panda_link_metadata& c, format_context& ctx)
      -> decltype(ctx.out());
};
