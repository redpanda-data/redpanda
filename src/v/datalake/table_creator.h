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

#include "datalake/catalog_schema_manager.h"
#include "datalake/record_schema_resolver.h"

namespace datalake {

class table_creator {
public:
    enum class errc {
        incompatible_schema,
        // The operation failed because of a subsystem failure.
        failed,
        // The system is shutting down.
        shutting_down,
    };
    friend std::ostream& operator<<(std::ostream&, const errc&);

    virtual ss::future<checked<std::nullopt_t, errc>> ensure_table(
      const model::topic&,
      model::revision_id topic_revision,
      record_schema_components) const
      = 0;

    virtual ~table_creator() = default;
};

// Creates or alters the table by interfacing directly with a catalog.
class direct_table_creator : public table_creator {
public:
    direct_table_creator(type_resolver&, schema_manager&);

    ss::future<checked<std::nullopt_t, errc>> ensure_table(
      const model::topic&,
      model::revision_id topic_revision,
      record_schema_components) const final;

private:
    datalake::type_resolver& type_resolver_;
    datalake::schema_manager& schema_mgr_;
};

} // namespace datalake
