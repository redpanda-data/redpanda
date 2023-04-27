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

#include "cluster/fwd.h"
#include "kafka/protocol/errors.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "outcome.h"
#include "pandaproxy/schema_registry/api.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

namespace pandaproxy::schema_registry {

class schema_id_validator {
public:
    class impl;
    schema_id_validator(
      const std::unique_ptr<api>& api,
      const model::topic& topic,
      const cluster::topic_properties& props);
    schema_id_validator(schema_id_validator&&) noexcept;
    schema_id_validator(const schema_id_validator&) = delete;
    schema_id_validator& operator=(schema_id_validator&&) = delete;
    schema_id_validator& operator=(const schema_id_validator&) = delete;
    ~schema_id_validator() noexcept;

    using result = ::result<model::record_batch_reader, kafka::error_code>;
    ss::future<result> operator()(model::record_batch_reader&&);

private:
    std::unique_ptr<impl> _impl;
};

std::optional<schema_id_validator> maybe_make_schema_id_validator(
  const std::unique_ptr<api>& api,
  const model::topic& topic,
  const cluster::topic_properties& props);

ss::future<schema_id_validator::result> inline maybe_validate_schema_id(
  std::optional<schema_id_validator> validator,
  model::record_batch_reader rbr) {
    if (validator) {
        co_return co_await (*validator)(std::move(rbr));
    }
    co_return std::move(rbr);
}

} // namespace pandaproxy::schema_registry
