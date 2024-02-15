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
#include "utils/named_type.h"

#include <seastar/core/sharded.hh>

#include <type_traits>
#include <variant>

class retry_chain_node;

namespace cloud_storage {
class cloud_storage_api;
enum class upload_result;
} // namespace cloud_storage

namespace cloud_storage::inventory {

// The identifier for a specific report configuration scheduled to run at a
// fixed frequency and producing files of a fixed format.
using inventory_config_id = named_type<ss::sstring, struct inventory_config>;

enum class report_generation_frequency { daily };
std::ostream& operator<<(std::ostream&, report_generation_frequency);

enum class report_format { csv };
std::ostream& operator<<(std::ostream&, report_format);

/// \brief This class is not directly used for runtime polymorphism, it exists
/// as a convenience to define constraints for inv_ops_variant, to make sure
/// that the classes set as variants of inv_ops_variant have the expected set of
/// methods defined in base_ops.
class base_ops {
public:
    virtual ss::future<cloud_storage::upload_result>
    create_inventory_configuration(
      cloud_storage::cloud_storage_api&,
      retry_chain_node&,
      report_generation_frequency,
      report_format)
      = 0;
};

template<typename T>
concept vendor_ops_provider = std::is_base_of_v<base_ops, T>;

template<vendor_ops_provider... Ts>
using inv_ops_variant = std::variant<Ts...>;

} // namespace cloud_storage::inventory
