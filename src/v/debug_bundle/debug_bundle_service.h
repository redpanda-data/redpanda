/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"
#include "config/property.h"
#include "debug_bundle/error.h"

#include <seastar/core/sharded.hh>

namespace debug_bundle {

/**
 * @brief Service used to manage creation of debug bundles
 *
 * This service is used to create, delete, and manage debug bundles using the
 * "rpk debug bundle" application
 */
class service final : public ss::peering_sharded_service<service> {
public:
    /// Default shard operations will be performed on
    static constexpr ss::shard_id service_shard = 0;
    /// Name of the debug bundle directory
    static constexpr std::string_view debug_bundle_dir_name = "debug-bundle";
    /**
     * @brief Construct a new debug bundle service object
     *
     * @param data_dir Path to the Redpanda data directory
     */
    explicit service(const std::filesystem::path& data_dir);
    /**
     * @brief Starts the service
     *
     * Starting the service will:
     * * Create the debug bundle directory
     * * Verify that the rpk binary is present
     */
    ss::future<> start();
    /**
     * @brief Halts the service
     */
    ss::future<> stop();

private:
    /// Path to the debug bundle directory
    std::filesystem::path _debug_bundle_dir;
    /// Binding called when the rpk path config changes
    config::binding<std::filesystem::path> _rpk_path_binding;
};
} // namespace debug_bundle
