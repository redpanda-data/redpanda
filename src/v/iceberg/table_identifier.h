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

#include "base/seastarx.h"
#include "container/chunked_vector.h"

#include <seastar/core/sstring.hh>

#include <boost/container_hash/hash.hpp>

namespace iceberg {
struct table_identifier {
    chunked_vector<ss::sstring> ns;
    ss::sstring table;

    table_identifier copy() const {
        return table_identifier{
          .ns = ns.copy(),
          .table = table,
        };
    }

    bool operator==(const table_identifier& other) const = default;
};
std::ostream& operator<<(std::ostream& o, const table_identifier& id);
} // namespace iceberg

namespace std {

template<>
struct hash<iceberg::table_identifier> {
    size_t operator()(const iceberg::table_identifier& table_id) const {
        size_t h = 0;
        for (const auto& ns : table_id.ns) {
            boost::hash_combine(h, std::hash<ss::sstring>()(ns));
        }
        boost::hash_combine(h, std::hash<ss::sstring>()(table_id.table));
        return h;
    };
};

} // namespace std
