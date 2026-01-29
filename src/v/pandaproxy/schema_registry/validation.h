/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/seastarx.h"
#include "cluster/topic_properties.h"
#include "kafka/protocol/errors.h"
#include "model/fundamental.h"
#include "pandaproxy/schema_registry/api.h"
#include "pandaproxy/schema_registry/schema_id_validation.h"

#include <seastar/core/future.hh>

namespace pandaproxy::schema_registry {

class schema_id_validator {
public:
    class impl;
    schema_id_validator(
      const std::unique_ptr<api>& api,
      const model::topic& topic,
      const cluster::topic_properties& props,
      pandaproxy::schema_registry::schema_id_validation_mode mode);
    schema_id_validator(schema_id_validator&&) noexcept;
    schema_id_validator(const schema_id_validator&) = delete;
    schema_id_validator& operator=(schema_id_validator&&) = delete;
    schema_id_validator& operator=(const schema_id_validator&) = delete;
    ~schema_id_validator() noexcept;

    ss::future<kafka::error_code> operator()(const model::record_batch&);

private:
    std::unique_ptr<impl> _impl;
};

std::optional<schema_id_validator> maybe_make_schema_id_validator(
  const std::unique_ptr<api>& api,
  const model::topic& topic,
  const cluster::topic_properties& props);

} // namespace pandaproxy::schema_registry
