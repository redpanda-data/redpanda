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
        using enum cloud_storage::inventory::inventory_creation_result;
    case success:
        return os << "success";
    case failed:
        return os << "failed";
    case already_exists:
        return os << "already-exists";
    }
}

} // namespace cloud_storage::inventory
