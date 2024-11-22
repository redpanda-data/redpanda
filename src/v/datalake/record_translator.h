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
#include "datalake/record_schema_resolver.h"
#include "datalake/schema_identifier.h"
#include "iceberg/datatypes.h"
#include "iceberg/values.h"
#include "model/timestamp.h"

#include <seastar/core/future.hh>

namespace datalake {

struct record_type {
    record_schema_components comps;
    iceberg::struct_type type;
};

class record_translator {
public:
    enum class errc {
        translation_error,
        unexpected_schema,
    };
    friend std::ostream& operator<<(std::ostream&, const errc&);

    virtual record_type build_type(std::optional<resolved_type> val_type) = 0;
    virtual ss::future<checked<iceberg::struct_value, errc>> translate_data(
      model::partition_id pid,
      kafka::offset o,
      std::optional<iobuf> key,
      const std::optional<resolved_type>& val_type,
      std::optional<iobuf> parsable_val,
      model::timestamp ts,
      const chunked_vector<
        std::pair<std::optional<iobuf>, std::optional<iobuf>>>& headers)
      = 0;
    virtual ~record_translator() = default;
};

class key_value_translator : public record_translator {
public:
    record_type build_type(std::optional<resolved_type> val_type) override;
    ss::future<checked<iceberg::struct_value, errc>> translate_data(
      model::partition_id pid,
      kafka::offset o,
      std::optional<iobuf> key,
      const std::optional<resolved_type>& val_type,
      std::optional<iobuf> parsable_val,
      model::timestamp ts,
      const chunked_vector<
        std::pair<std::optional<iobuf>, std::optional<iobuf>>>& headers)
      override;
    ~key_value_translator() override = default;
};

class structured_data_translator : public record_translator {
public:
    record_type build_type(std::optional<resolved_type> val_type) override;
    ss::future<checked<iceberg::struct_value, errc>> translate_data(
      model::partition_id pid,
      kafka::offset o,
      std::optional<iobuf> key,
      const std::optional<resolved_type>& val_type,
      std::optional<iobuf> parsable_val,
      model::timestamp ts,
      const chunked_vector<
        std::pair<std::optional<iobuf>, std::optional<iobuf>>>& headers)
      override;
    ~structured_data_translator() override = default;
};

// Switches between key-value and structured translator, depending on if there
// is an input schema.
// XXX: this is a temporary hack for tests to pass as we transition to toggling
// mode with a topic config! Instead, callers should explicitly choose.
class default_translator : public record_translator {
public:
    record_type build_type(std::optional<resolved_type> val_type) override;
    ss::future<checked<iceberg::struct_value, errc>> translate_data(
      model::partition_id pid,
      kafka::offset o,
      std::optional<iobuf> key,
      const std::optional<resolved_type>& val_type,
      std::optional<iobuf> parsable_val,
      model::timestamp ts,
      const chunked_vector<
        std::pair<std::optional<iobuf>, std::optional<iobuf>>>& headers)
      override;
    ~default_translator() override = default;

private:
    key_value_translator kv_translator;
    structured_data_translator structured_translator;
};

} // namespace datalake
