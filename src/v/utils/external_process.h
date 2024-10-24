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
#include "base/vassert.h"
#include "utils/mutex.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/iostream.hh>
#include <seastar/util/process.hh>

#include <chrono>
#include <optional>
#include <system_error>

namespace external_process {
/**
 * @brief Defines the error codes emited by this utility
 */
enum class error_code : int {
    success = 0,
    process_already_completed,
    process_already_being_waited_on,
    process_already_being_terminated,
    failed_to_terminate_process,
    terminate_called_before_wait
};

/**
 * @brief Defines the external_process::error_category
 */
struct error_category final : public std::error_category {
    /**
     * @return const char* Name of the category
     */
    const char* name() const noexcept final {
        return "external_process::error_category";
    }

    /**
     * @param c Error code to stringify
     * @return std::string Stringified version of @p c
     */
    std::string message(int c) const final {
        switch (static_cast<error_code>(c)) {
        case error_code::success:
            return "success";
        case error_code::process_already_completed:
            return "process already completed";
        case error_code::process_already_being_waited_on:
            return "wait already called on process";
        case error_code::process_already_being_terminated:
            return "terminate already being called on the process";
        case error_code::failed_to_terminate_process:
            return "failed to terminate the process";
        case error_code::terminate_called_before_wait:
            return "termiate called before wait";
        }

        return "(unknown error code)";
    }
};

/**
 * @return const std::error_category& Returns static instance of
 * external_process::error_category
 */
inline const std::error_category& error_category() noexcept {
    static struct error_category e;
    return e;
}
inline std::error_code make_error_code(error_code e) noexcept {
    return {static_cast<int>(e), error_category()};
}

/**
 * @brief This class wraps a Seastar experimental process instance
 *
 * The purpose of this class is to provide an easy way of controlling a spawned
 * child process.
 */
class external_process final {
public:
    /**
     * @brief Create a external process object
     *
     * This method will return a created external_process object with an already
     * spawned process.  It will attach any provided consumer to the appropriate
     * ss::input_stream<char>.
     *
     * @note You _must_ call `wait()` on the returned object before the object
     * is destructed.
     *
     * @param command The command to run with its arguments, divided over the
     * vector.  Will assert if empty.
     * @param env Optionall provided a list of environmental variables as
     * KEY=VALUE
     * @return A std::unique_ptr containing the created external process
     * @throw std::system_error If unable to launch the process
     */
    static ss::future<std::unique_ptr<external_process>>
    create_external_process(
      std::vector<ss::sstring> command,
      std::optional<std::vector<ss::sstring>> env = std::nullopt);

    external_process() = delete;
    external_process(const external_process&) = delete;
    external_process& operator=(const external_process&) = delete;
    external_process(external_process&&) noexcept = delete;
    external_process& operator=(external_process&&) noexcept = delete;

    ~external_process();

    /**
     * @return true If process is still running, false otherwise
     */
    bool is_running() const { return !_completed; }

    /**
     * @return const std::filesystem::path& Path to the executable being
     * executed
     */
    const std::filesystem::path& path() const noexcept { return _path; }
    /**
     * @return const ss::experimental::spawn_parameters& The spawn parameters
     */
    const ss::experimental::spawn_parameters& parameters() const noexcept {
        return _params;
    }

    /**
     * @brief Set the stdout consumer object
     *
     * @tparam Consumer The type of consumer, must satisfiy the requirements of
     * ss::InputStreamConsumer
     * @param consumer Instance of the consumer that will receive the output of
     * stdout
     * @note Will assert if called >1
     */
    template<typename Consumer>
    requires ss::InputStreamConsumer<Consumer, char>
    void set_stdout_consumer(Consumer consumer) {
        vassert(!_stdout_consumer.has_value(), "Already set stdout consumer");
        auto cout = _process.cout();
        _stdout_consumer.emplace(ss::do_with(
          std::move(cout),
          [consumer = std::move(consumer)](
            ss::input_stream<char>& cout) mutable {
              return cout.consume(std::move(consumer));
          }));
    }

    /**
     * @brief Set the stderr consumer object
     *
     * @tparam Consumer The type of consumer, must satisfiy the requirements of
     * ss::InputStreamConsumer
     * @param consumer Instance of the consumer that will receive the output of
     * stderr
     * @note Will assert if called >1
     */
    template<typename Consumer>
    requires ss::InputStreamConsumer<Consumer, char>
    void set_stderr_consumer(Consumer consumer) {
        vassert(!_stderr_consumer.has_value(), "Already set stdout consumer");
        auto cerr = _process.cerr();
        _stderr_consumer.emplace(ss::do_with(
          std::move(cerr),
          [consumer = std::move(consumer)](
            ss::input_stream<char>& cerr) mutable {
              return cerr.consume(std::move(consumer));
          }));
    }

    /**
     * @brief Waits for the process to finish executing
     *
     * This will in turn call wait(2) on the child process, awaiting for it
     * to complete.  This function _must_ be called on all created
     * `external_process` objects.  This can only be called once.
     *
     * @return ss::future<ss::experimental::process::wait_status> The result of
     * the process
     * @throw std::system_error Containing an `external_process::error_code`:
     * * `external_process::process_already_completed` if process already
     *    completed
     * * `process_already_being_waited_on` if `wait()` has already been called
     */
    ss::future<ss::experimental::process::wait_status> wait();

    /**
     * @brief Attempts to call SIGTERM and then SIGKILL on the running process
     *
     * @note Must call this _after_ calling `wait()` and while awaiting for
     * `wait()`'s future to complete.
     *
     * @param timeout How long to wait until calling SIGKILL if SIGTERM fails
     * @throw std::system_error Containing an `external_process::errro_code`
     * * `external_process::process_already_completed` if process already done
     * * `external_process::process_already_being_terminated` if `terminate()`
     *    already called
     * * `external_process::failed_to_terminate_process` if unable to signal
     *    process
     * * `external_process::terminate_called_before_wait` if `terminate()`
     *    called before `wait()`
     */
    ss::future<> terminate(std::chrono::milliseconds timeout);

private:
    external_process(
      ss::experimental::process p,
      std::filesystem::path path,
      ss::experimental::spawn_parameters params);

private:
    // The external process
    ss::experimental::process _process;
    // Path to the executable that's being executed
    std::filesystem::path _path;
    // The parameters for the process
    ss::experimental::spawn_parameters _params;
    // Flag indicating that the external process has completed
    bool _completed{false};
    // Protects against multiple calls to `terminate()`
    mutex _terminate_mutex;
    // Protects against multiple calls to `wait()`
    mutex _wait_mutex;
    // Holds the future of the stdout consumer which will be cleaned up in
    // wait()
    std::optional<ss::future<>> _stdout_consumer{std::nullopt};
    // Holds the future of the stderr consumer which will be cleaned up in
    // wait()
    std::optional<ss::future<>> _stderr_consumer{std::nullopt};
    ss::abort_source term_as;
    ss::gate _gate;
};

} // namespace external_process

namespace std {
template<>
struct is_error_code_enum<external_process::error_code> : true_type {};
} // namespace std
