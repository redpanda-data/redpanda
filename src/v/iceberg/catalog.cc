/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "iceberg/catalog.h"

#include <fmt/format.h>

namespace iceberg {
std::ostream& operator<<(std::ostream& o, const table_identifier& id) {
    return o << fmt::format(
             "{{ns: {}, table: {}}}", fmt::join(id.ns, "/"), id.table);
}
std::ostream& operator<<(std::ostream& o, catalog::errc e) {
    switch (e) {
    case catalog::errc::io_error:
        return o << "catalog::errc::io_error";
    case catalog::errc::timedout:
        return o << "catalog::errc::timedout";
    case catalog::errc::unexpected_state:
        return o << "catalog::errc::unexpected_state";
    case catalog::errc::shutting_down:
        return o << "catalog::errc::shutting_down";
    case catalog::errc::already_exists:
        return o << "catalog::errc::already_exists";
    case catalog::errc::not_found:
        return o << "catalog::errc::not_found";
    }
}
} // namespace iceberg
