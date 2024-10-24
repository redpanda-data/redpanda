// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "iceberg/action.h"
#include "iceberg/schema.h"
#include "iceberg/table_metadata.h"

namespace iceberg {

// Action for updating the current schema to the given schema.
// TODO: only adding new columns works. Handle removing or altering columns.
class update_schema_action : public action {
public:
    update_schema_action(const table_metadata& table, schema new_schema)
      : table_(table)
      , new_schema_(std::move(new_schema)) {}

protected:
    ss::future<action_outcome> build_updates() && final;

private:
    const table_metadata& table_;
    schema new_schema_;
};

} // namespace iceberg
