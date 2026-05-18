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

#include "datalake/catalog_schema_manager.h"
#include "datalake/record_schema_resolver.h"
#include "datalake/table_creator.h"

namespace datalake {

// Hourly partitioning on the redpanda.timestamp field.
iceberg::unresolved_partition_spec hour_partition_spec();

// Daily partitioning on the redpanda.timestamp field.
iceberg::unresolved_partition_spec day_partition_spec();

// Creates or alters the table by interfacing directly with a catalog.
class direct_table_creator : public table_creator {
public:
    direct_table_creator(type_resolver&, schema_manager&);

    ss::future<checked<std::nullopt_t, errc>> ensure_table(
      const model::topic&,
      model::revision_id topic_revision,
      record_schema_components) const final;

    ss::future<checked<std::nullopt_t, errc>> ensure_dlq_table(
      const model::topic&, model::revision_id topic_revision) const final;

private:
    type_resolver& type_resolver_;
    schema_manager& schema_mgr_;
};

} // namespace datalake
