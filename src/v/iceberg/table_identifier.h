// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "base/seastarx.h"
#include "container/fragmented_vector.h"
#include "serde/envelope.h"
#include "serde/rw/sstring.h"
#include "serde/rw/vector.h"

#include <seastar/core/sstring.hh>

#include <boost/container_hash/hash.hpp>

namespace iceberg {
struct table_identifier
  : serde::
      envelope<table_identifier, serde::version<0>, serde::compat_version<0>> {
    chunked_vector<ss::sstring> ns;
    ss::sstring table;

    table_identifier copy() const {
        return table_identifier{
          .ns = ns.copy(),
          .table = table,
        };
    }

    bool operator==(const table_identifier& other) const = default;

    auto serde_fields() { return std::tie(ns, table); }
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
