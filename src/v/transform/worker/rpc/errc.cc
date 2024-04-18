/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "transform/worker/rpc/errc.h"

namespace transform::worker::rpc {

const char* errc_category::name() const noexcept {
    return "transform::worker::rpc::errc";
}

std::string errc_category::message(int c) const {
    switch (static_cast<errc>(c)) {
    case errc::success:
        return "Success";
    case errc::failed_to_start_engine:
        return "Failed to start engine";
    case errc::vm_not_found:
        return "VM not found";
    case errc::transform_failed:
        return "Transform failed";
    }
    return "transform::worker::rpc::errc::unknown(" + std::to_string(c) + ")";
}

} // namespace transform::worker::rpc
