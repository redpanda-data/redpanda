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
    };
    friend std::ostream& operator<<(std::ostream&, const errc&);
    static record_type build_type(std::optional<resolved_type> val_type);
    static ss::future<checked<iceberg::struct_value, errc>> translate_data(
      kafka::offset o,
      iobuf key,
      const std::optional<resolved_type>& val_type,
      iobuf parsable_val,
      model::timestamp ts);
};

} // namespace datalake
