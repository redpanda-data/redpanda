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
#include "debug_bundle/types.h"
#include "storage/fwd.h"
#include "utils/mutex.h"

#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/timer.hh>
#include <seastar/util/process.hh>

namespace debug_bundle {

/// Forward declare probe
class probe;

/**
 * @brief Service used to manage creation of debug bundles
 *
 * This service is used to create, delete, and manage debug bundles using the
 * "rpk debug bundle" application
 */
class service final : public ss::peering_sharded_service<service> {
public:
    /// Name of the debug bundle directory
    static constexpr std::string_view debug_bundle_dir_name = "debug-bundle";
    /// Key used to store metadata in the kvstore
    static constexpr std::string_view debug_bundle_metadata_key
      = "debug_bundle_metadata";
    /**
     * @brief Construct a new debug bundle service object
     */
    explicit service(storage::kvstore* kvstore);

    /// Destructor
    ~service() noexcept;
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

    /**
     * @brief Initializes the creation of a debug bundle
     *
     * @param job_id The job ID
     * @param params the parameters
     * @return Result with possible error codes:
     * * error_code::debug_bundle_process_running
     * * error_code::invalid_parameters
     * * error_code::process_failed
     * * error_code::internal_error
     */
    ss::future<result<void>> initiate_rpk_debug_bundle_collection(
      job_id_t job_id, debug_bundle_parameters params);

    /**
     * @brief Attempts to cancel a running "rpk debug bundle" process
     *
     * @param job_id The Job ID to cancel
     * @return ss::future<result<void>> The result with possible error codes:
     * * error_code::debug_bundle_process_not_running
     * * error_code::internal_error
     * * error_code::job_id_not_recognized
     * * error_code::debug_bundle_process_never_started
     */
    ss::future<result<void>> cancel_rpk_debug_bundle(job_id_t job_id);

    /**
     * @brief Retrieves the status of the bundle process
     *
     * @return ss::future<result<debug_bundle_status_data>> The result with
     * possible error codes:
     * * error_code::debug_bundle_process_never_started
     */
    ss::future<result<debug_bundle_status_data>> rpk_debug_bundle_status();

    /**
     * @brief Returns the path to the debug bundle file
     * @param job_id The job id of the file to get
     *
     * @return ss::future<result<std::filesystem::path>> The result with
     * possible error codes:
     * * error_code::debug_bundle_process_running - The process is still running
     * * error_code::process_failed - The process errored out so no file
     *   available
     * * error_code::debug_bundle_process_never_started
     */
    ss::future<result<std::filesystem::path>>
    rpk_debug_bundle_path(job_id_t job_id);

    /**
     * @brief Attempts to delete the debug bundle file
     * @param job_id The job id of the file to delete
     *
     * @return ss::future<result<void>> The result with possible error codes:
     * * error_code::debug_bundle_process_never_started
     * * error_code::debug_bundle_process_running - The process is still running
     * * error_code::internal_error
     */
    ss::future<result<void>> delete_rpk_debug_bundle(job_id_t job_id);

    /// Returns the current debug bundle directory
    const std::filesystem::path& get_debug_bundle_output_directory() const {
        return _debug_bundle_dir;
    }

private:
    /**
     * @brief Constructs the arguments for the rpk debug bundle command
     *
     * @param debug_bundle_file_path Path to where to output the debug bundle
     * @param params parameters
     * @return std::vector<ss::sstring> The list of strings to pass to
     * external_process
     */
    result<std::vector<ss::sstring>> build_rpk_arguments(
      std::string_view debug_bundle_file_path, debug_bundle_parameters params);

    /**
     * @brief Cleans up files from previous debug bundle run
     */
    ss::future<> cleanup_previous_run() const;

    /**
     * @brief Removes debug bundle entry from kvstore
     */
    ss::future<> remove_kvstore_entry() const;

    /**
     * @brief Set the metadata object within the kvstore
     *
     * @param job_id The job id
     */
    ss::future<> set_metadata(job_id_t job_id);

    /**
     * @brief Returns the status of the running process
     *
     * @return std::optional<debug_bundle_status> Will return std::nullopt if
     * process was never executed, else will return the debug_bundle_status
     */
    std::optional<debug_bundle_status> process_status() const;

    /**
     * @return Whether or not the RPK debug bundle process is running
     */
    bool is_running() const;

    /**
     * @brief Handles the result after waiting on the rpk debug bundle process
     *
     * @param job_id The associated job ID
     */
    ss::future<> handle_wait_result(job_id_t job_id);

    /**
     * @brief Attempts to reload a previous run
     *
     * If a previous run exists in the kvstore upon service start, will attempt
     * to reload the metadata and make that debug bundle available to the user
     *
     */
    ss::future<> maybe_reload_previous_run();

    /**
     * @brief Called when the timer expires
     *
     * Will check to see if the debug bundle has expired and remove it
     */
    ss::future<> tick();

    /**
     * @brief Called whenever the debug_bundle_auto_removal_seconds value has
     * been modified
     *
     * This function will cancel the timer if the value is std::nullopt,
     * otherwise will re-calculate the timer's expiration based on if a debug
     * bundle has been generated
     */
    void maybe_rearm_timer();

private:
    /// Handler used to emplace stdout/stderr into a buffer
    struct output_handler;
    /// KV store used to store metadata
    storage::kvstore* _kvstore;
    /// Structure used to hold information about the running rpk debug bundle
    /// process
    class debug_bundle_process;
    /// Path to the debug bundle directory
    std::filesystem::path _debug_bundle_dir;
    /// Binding to debug bundle storage directory config
    config::binding<std::optional<std::filesystem::path>>
      _debug_bundle_storage_dir_binding;
    /// Binding called when the rpk path config changes
    config::binding<std::filesystem::path> _rpk_path_binding;
    /// Binding called when the debug bundle cleanup configuration has changed
    config::binding<std::optional<std::chrono::seconds>>
      _debug_bundle_cleanup_binding;
    /// External process
    std::unique_ptr<debug_bundle_process> _rpk_process;
    /// Metrics probe
    std::unique_ptr<probe> _probe;
    /// Timer used to clean up previous runs of the debug bundle
    ss::timer<ss::lowres_clock> _cleanup_timer;
    /// Mutex to guard control over the rpk debug bundle process
    mutex _process_control_mutex;
    ss::gate _gate;
};
} // namespace debug_bundle
