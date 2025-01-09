// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "iceberg/table_identifier.h"
#include "model/fundamental.h"

namespace datalake {

class table_identifier_provider {
public:
    iceberg::table_identifier table_id(const model::topic& t) const {
        return iceberg::table_identifier{
          // TODO: namespace as a topic property? Keep it in the table metadata?
          .ns = {"redpanda"},
          .table = t,
        };
    }
};

}; // namespace datalake
