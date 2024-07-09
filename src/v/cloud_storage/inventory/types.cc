/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/inventory/types.h"

#include "cloud_storage/configuration.h"
#include "config/node_config.h"
#include "model/metadata.h"

namespace {
constexpr auto supported_backends = {model::cloud_storage_backend::aws};
}

namespace cloud_storage::inventory {
std::ostream& operator<<(std::ostream& os, report_generation_frequency rgf) {
    switch (rgf) {
    case report_generation_frequency::daily:
        return os << "Daily";
    }
}

std::ostream& operator<<(std::ostream& os, report_format rf) {
    switch (rf) {
    case report_format::csv:
        return os << "CSV";
    }
}

std::ostream& operator<<(std::ostream& os, inventory_creation_result icr) {
    switch (icr) {
        using enum inventory_creation_result;
    case success:
        return os << "success";
    case already_exists:
        return os << "already-exists";
    }
}

bool validate_backend_supported_for_inventory_scrub(
  model::cloud_storage_backend backend) {
    return std::ranges::find(supported_backends, backend)
           != supported_backends.end();
}

} // namespace cloud_storage::inventory
