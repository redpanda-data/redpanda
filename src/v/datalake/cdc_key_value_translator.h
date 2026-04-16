/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "datalake/record_translator.h"

namespace datalake {

/// Translates records using key_value semantics with CDC: every record
/// produces an equality delete for its key, and tombstones (null value)
/// are treated as pure deletes.
class cdc_key_value_translator : public record_translator {
public:
    record_type
    build_type(std::optional<shared_resolved_type_t> val_type) override;

    ss::future<checked<translated_record, errc>> translate_data(
      model::partition_id pid,
      kafka::offset o,
      std::optional<iobuf> key,
      const std::optional<shared_resolved_type_t>& val_type,
      std::optional<iobuf> parsable_val,
      model::timestamp ts,
      model::timestamp_type ts_t,
      const chunked_vector<model::record_header>& headers) override;

    ~cdc_key_value_translator() override = default;
};

} // namespace datalake
