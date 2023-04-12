/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "vlog.h"

namespace detail {

class structured_error_code {
public:
    constexpr explicit structured_error_code(const char* cat, uint16_t i)
      : category(cat)
      , number(i) {}

    const char* category;
    const uint16_t number;
};
} // namespace detail

/**
 * ======================
 * Error code definitions
 * ======================
 *
 * These are defined centrally in this file for all subsystems, so that we
 * can easily avoid accidentally re-using codes for different subsystems.
 */

/**
 * Soft assertions are cases that should never happen if our code is bug free,
 * but which can be recovered and do not necessitate panicking the process.
 */
namespace soft_assert {
constexpr const char* prefix = "ESA";

// We violated internal rules about where start_offset may be set relative
// to existent segments.
constexpr auto cloud_storage_start_offset_out_of_range
  = detail::structured_error_code(prefix, 1);

// We failed to cover all the error types that could occur during a call
// to object storage.
constexpr auto cloud_storage_unhandled_client_error
  = detail::structured_error_code(prefix, 2);

} // namespace soft_assert

/**
 * I/O errors are cases that indicate an external system is misbehaving in a way
 * that prevents us from doing our job, for example if a disk gives us an EIO
 * or if a cloud storage API behaves outside its spec.
 */
namespace io_error {
constexpr const char* prefix = "EIO";

// Operating system indicates I/O error on block device, we cannot proceed.
constexpr auto local_disk_eio = detail::structured_error_code(prefix, 1);

// Failure reading while doing topic recovery
constexpr auto cloud_storage_recovery_fatal = detail::structured_error_code(
  prefix, 2);

// Failure to load data from remote storage to serve a read
constexpr auto cloud_storage_hydration_fatal = detail::structured_error_code(
  prefix, 3);

// Backend sent a malformed response body, not compliant with S3 spec
constexpr auto cloud_storage_bad_response = detail::structured_error_code(
  prefix, 4);

} // namespace io_error

/**
 * Consistency errors may indicate corruption in storage.  For example if
 * a cloud storage manifest refers to a segment that does not exist.  This
 * can be a bug in redpanda, a bug in the storage backend, or external
 * interference.
 */
namespace consistency_error {
constexpr const char* prefix = "ECR";
constexpr auto cloud_storage_missing_segment = detail::structured_error_code(
  prefix, 1);
} // namespace consistency_error

/**
 * Configuration errors indicate that while configuration might have been
 * syntactically correct, something is wrong with it in practice: for example
 * we are configured to use an object storage bucket that does not exist.
 */
namespace configuration_error {
constexpr const char* prefix = "ECN";
constexpr auto cloud_storage_missing_bucket = detail::structured_error_code(
  prefix, 1);

} // namespace configuration_error

/**
 * Structured error logging: pass a subclass of `structured_error_code` as
 * the code, so that the error can be uniquely mapped back to a specific
 * condition in a way that is robust across versions of Redpanda and does
 * not rely on the exact formatting of the string, the line number, or
 * the naming of loggers.
 *
 * Prefer this to using plain `vlog` for errors.
 */
// NOLINTNEXTLINE
#define vlog_error(logger, code, fmt, args...)                                 \
    fmt_with_ctx_level(                                                        \
      logger,                                                                  \
      ss::log_level::error,                                                    \
      "code={}{:05d} " fmt,                                                    \
      code.category,                                                           \
      code.number,                                                             \
      ##args)
