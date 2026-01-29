/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include <fmt/format.h>
#include <fmt/std.h>

#include <exception>
#include <stdexcept>

namespace lsm {

// The base exception for all lsm errors.
class base_exception : public std::runtime_error {
protected:
    explicit base_exception(const std::string& msg)
      : std::runtime_error(msg) {}
};

// An exception for when an invalid argument was passed in
class invalid_argument_exception : public base_exception {
public:
    template<typename... T>
    explicit invalid_argument_exception(
      fmt::format_string<T...> msg, T&&... args)
      : base_exception(fmt::format(msg, std::forward<T>(args)...)) {}
};

// An exception for when corruption was detected within the database.
class corruption_exception : public base_exception {
public:
    template<typename... T>
    explicit corruption_exception(fmt::format_string<T...> msg, T&&... args)
      : base_exception(fmt::format(msg, std::forward<T>(args)...)) {}
};

// An exception for when there is an error performing IO.
class io_error_exception : public base_exception {
public:
    template<typename... T>
    explicit io_error_exception(fmt::format_string<T...> msg, T&&... args)
      : base_exception(fmt::format(msg, std::forward<T>(args)...))
      , _error_code(std::make_error_code(std::errc::io_error)) {}

    template<typename... T>
    explicit io_error_exception(
      const std::error_code& code, fmt::format_string<T...> msg, T&&... args)
      : base_exception(fmt::format(msg, std::forward<T>(args)...))
      , _error_code(code) {}

    std::error_code code() const { return _error_code; }

private:
    std::error_code _error_code;
};

// An exception for when the database is being shutdown or an abort was
// requested.
class abort_requested_exception : public base_exception {
public:
    template<typename... T>
    explicit abort_requested_exception(
      fmt::format_string<T...> msg, T&&... args)
      : base_exception(fmt::format(msg, std::forward<T>(args)...)) {}
};

} // namespace lsm
