/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "json/document.h"
#include "json/json.h"
#include "json/stringbuffer.h"
#include "json/writer.h"
#include "likely.h"
#include "pandaproxy/schema_registry/errors.h"
#include "pandaproxy/schema_registry/types.h"
#include "seastarx.h"
#include "ssx/semaphore.h"

#include <fmt/core.h>
#include <fmt/format.h>
#include <rapidjson/error/en.h>

#include <exception>
#include <utility>

namespace pandaproxy::schema_registry {

///\brief Create a schema definition from raw input.
///
/// Validates the JSON and minify it.
/// TODO(Ben): Validate that it's a valid schema
///
/// Returns error_code::schema_invalid on failure
template<typename Encoding>
result<unparsed_schema_definition::raw_string>
make_schema_definition(std::string_view sv) {
    // Validate and minify
    // TODO (Ben): Minify. e.g.:
    // "schema": "{\"type\": \"string\"}" -> "schema": "\"string\""
    ::json::GenericDocument<Encoding> doc;
    doc.Parse(sv.data(), sv.size());
    if (doc.HasParseError()) {
        return error_info{
          error_code::schema_invalid,
          fmt::format(
            "Invalid schema: {} at offset {}",
            rapidjson::GetParseError_En(doc.GetParseError()),
            doc.GetErrorOffset())};
    }
    ::json::GenericStringBuffer<Encoding> str_buf;
    str_buf.Reserve(sv.size());
    ::json::Writer<::json::GenericStringBuffer<Encoding>> w{str_buf};
    doc.Accept(w);
    return unparsed_schema_definition::raw_string{
      ss::sstring{str_buf.GetString(), str_buf.GetSize()}};
}

///\brief The first invocation of one_shot::operator()() will invoke func and
/// wait for it to finish. Concurrent invocatons will also wait.
///
/// On success, all waiters will be allowed to continue. Successive invocations
/// of one_shot::operator()() will return ss::now().
///
/// If func fails, waiters will receive the error, and one_shot will be reset.
/// Successive calls to operator()() will restart the process.
class one_shot {
    enum class state { empty, started, available };
    using futurator = ss::futurize<ssx::semaphore_units>;

public:
    explicit one_shot(ss::noncopyable_function<ss::future<>()> func)
      : _func{std::move(func)} {}
    futurator::type operator()() {
        if (likely(_started_sem.available_units() != 0)) {
            return ss::get_units(_started_sem, 1);
        }
        auto units = ss::consume_units(_started_sem, 1);
        return _func().then_wrapped(
          [this, units{std::move(units)}](ss::future<> f) mutable noexcept {
              if (f.failed()) {
                  units.release();
                  auto ex = f.get_exception();
                  _started_sem.broken(ex);
                  _started_sem = {0, "pproxy/oneshot"};
                  return futurator::make_exception_future(ex);
              }

              _started_sem.signal(_started_sem.max_counter());
              return futurator::convert(std::move(units));
          });
    }

private:
    ss::noncopyable_function<ss::future<>()> _func;
    ssx::semaphore _started_sem{0, "pproxy/oneshot"};
};

} // namespace pandaproxy::schema_registry
