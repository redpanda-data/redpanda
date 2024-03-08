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

#include "base/outcome.h"
#include "cloud_storage_clients/types.h"
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

enum class error_outcome {
    success = 0,
    failed,
    manifest_download_failed,
    manifest_files_parse_failed,
    manifest_deserialization_failed,
};

struct error_outcome_category final : public std::error_category {
    const char* name() const noexcept final {
        return "cloud_storage::inventory::errc";
    }

    std::string message(int c) const final {
        switch (static_cast<error_outcome>(c)) {
        case error_outcome::success:
            return "Success";
        case error_outcome::failed:
            return "Failed";
        case error_outcome::manifest_download_failed:
            return "Failed to download manifest";
        case error_outcome::manifest_files_parse_failed:
            return "Failed to extract files object from manifest";
        case error_outcome::manifest_deserialization_failed:
            return "Failed to parse manifest to JSON";
        default:
            return fmt::format("Unknown outcome ({})", c);
        }
    }
};

inline const std::error_category& error_category() noexcept {
    static error_outcome_category e;
    return e;
}

inline std::error_code make_error_code(error_outcome e) noexcept {
    return {static_cast<int>(e), error_category()};
}

inline std::ostream& operator<<(std::ostream& o, error_outcome e) {
    o << error_category().message(static_cast<int>(e));
    return o;
}

// The identifier for a specific report configuration scheduled to run at a
// fixed frequency and producing files of a fixed format.
using inventory_config_id = named_type<ss::sstring, struct inventory_config>;

enum class report_generation_frequency { daily };
std::ostream& operator<<(std::ostream&, report_generation_frequency);

enum class report_format { csv };
std::ostream& operator<<(std::ostream&, report_format);

// A string is used instead of a chrono type because the strings returned by the
// vendor APIs are already roughly ISO-8601 formatted. This format is well
// suited for lexical sorting as well as creating directories on disk to store
// data for a given date, these are the only operations we need this type for.
using report_datetime = named_type<ss::sstring, struct report_datetime_t>;

// Stores a path to one or more reports. While in most cases one report run will
// generate just one CSV file, in some cases (e.g. Google with more than 1
// million objects) there may be more than one file per run. The result stores
// paths in a vector and the caller is made to handle multiple files per run to
// accomodate for these cases.
using report_paths = std::vector<cloud_storage_clients::object_key>;

struct report_metadata {
    cloud_storage_clients::object_key metadata_path;
    report_paths report_paths;
    report_datetime datetime;
};

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

    virtual ss::future<bool> inventory_configuration_exists(
      cloud_storage::cloud_storage_api& remote, retry_chain_node& parent_rtc)
      = 0;

    virtual ss::future<result<report_metadata, error_outcome>>
    latest_report_metadata(cloud_storage::cloud_storage_api&, retry_chain_node&)
      = 0;
};

template<typename T>
concept vendor_ops_provider = std::is_base_of_v<base_ops, T>;

template<vendor_ops_provider... Ts>
using inv_ops_variant = std::variant<Ts...>;

enum class inventory_creation_result {
    success,
    failed,
    already_exists,
};

std::ostream& operator<<(std::ostream&, inventory_creation_result);

} // namespace cloud_storage::inventory

namespace std {
template<>
struct is_error_code_enum<cloud_storage::inventory::error_outcome>
  : true_type {};
} // namespace std
