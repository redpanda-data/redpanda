/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "base/seastarx.h"
#include "compat/check.h"
#include "compat/run.h"
#include "json/prettywriter.h"
#include "model/compression.h"
#include "test_utils/tmp_dir.h"
#include "utils/directory_walker.h"
#include "utils/file_io.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

#include <limits>

namespace {
// NOLINTNEXTLINE(misc-no-recursion)
std::optional<bool>
perturb(json::Value& value, json::Document& doc, std::string& path) {
    auto perturb_int = [&]<typename T>(T v) -> std::optional<T> {
        /*
         * some integers represent enums. in the redpanda tree when an enum is
         * printed out in string form it is usually done via a case statement
         * that covers all of the enum values and a non-matching enum is a hard
         * fail as the switch falls through to a call to __builtin_unreachable.
         *
         * when the compat checker discovers a mismatch it prints out the
         * instances involved in the check. however, the random change made by
         * the perturb() function may produce an enum value that is out of
         * range, resulting in the __builtin_unreachable call and a crash.
         *
         * the following perturb helper for integers is a heuristic meant to be
         * play nice with enum fields. generally enum values start at 0, have no
         * gaps in values, and there are more than one value. so it is usually
         * safe to add one if the value is zero and subtract one otherwise.
         *
         * Other exceptions:
         * - acl principal type has only one enum value, so we can't change it.
         * - model::compression is an enum that has non contiguous values
         */
        if (path.find("entry.principal.type") != std::string::npos) {
            return std::nullopt;
        }
        if (path.find("compression") != std::string::npos) {
            // model::enum has non-contiguous range, whose upper bound is
            // uint8_t::max(), in the case this is found, reset 'v' to the
            // next lowest value
            using compress_ut = std::underlying_type_t<model::compression>;
            if (v == static_cast<compress_ut>(model::compression::producer)) {
                v = static_cast<compress_ut>(model::compression::zstd);
            }
        }
        const auto orig = v;
        const int modifier = (v == 0 || v == std::numeric_limits<T>::min())
                               ? 1
                               : -1;
        v += modifier;
        path = fmt::format("{} = {} ({}/{})", path, v, orig, modifier);
        return v;
    };
    if (value.IsInt()) {
        auto v = perturb_int(value.GetInt());
        if (!v.has_value()) {
            return std::nullopt;
        }
        value.SetInt(v.value());
        return true;
    }
    if (value.IsUint()) {
        auto v = perturb_int(value.GetUint());
        if (!v.has_value()) {
            return std::nullopt;
        }
        value.SetUint(v.value());
        return true;
    }
    if (value.IsInt64()) {
        auto v = perturb_int(value.GetInt64());
        if (!v.has_value()) {
            return std::nullopt;
        }
        value.SetInt64(v.value());
        return true;
    }
    if (value.IsUint64()) {
        auto v = perturb_int(value.GetUint64());
        if (!v.has_value()) {
            return std::nullopt;
        }
        value.SetUint64(v.value());
        return true;
    }
    if (value.IsBool()) {
        value.SetBool(!value.GetBool());
        path = fmt::format("{} = {} (!)", path, value.GetBool());
        return true;
    }
    if (value.IsString()) {
        const auto orig = value.GetString();

        // heuristic for addresses so the resulting string can be converted into
        // a ss::inet_address without throwing an exception
        std::string tmp;
        if (std::string("127.0.0.1") == orig) {
            tmp = "127.0.0.2";
        } else {
            tmp = fmt::format("{}x", orig);
        }

        value.SetString(tmp.c_str(), tmp.size(), doc.GetAllocator());
        path = fmt::format("{} = {} (was: {})", path, value.GetString(), orig);
        return true;
    }
    if (value.IsArray()) {
        size_t i = 0;
        for (auto& e : value.GetArray()) {
            auto tmp = fmt::format("{}[{}]", path, i++);
            if (perturb(e, doc, tmp).value_or(false)) {
                path = tmp;
                return true;
            }
        }
        if (!value.GetArray().Empty()) {
            value.GetArray().PopBack();
            return true;
        }
        return std::nullopt;
    }
    if (value.IsObject()) {
        auto change_feasible = false;
        for (auto it = value.MemberBegin(); it != value.MemberEnd(); ++it) {
            auto tmp = fmt::format("{}.{}", path, it->name.GetString());
            auto changed = perturb(it->value, doc, tmp);
            if (changed) {
                if (changed.value()) {
                    path = tmp;
                    return true;
                }
                change_feasible = true;
            }
        }
        if (change_feasible) {
            return false;
        }
        return std::nullopt;
    }
    if (value.IsNull()) {
        return std::nullopt;
    }
    vassert(
      false, "No perturb case defined for value type {}", value.GetType());
}

void check(std::filesystem::path fn) {
    /*
     * Normal test -- input passes.
     */
    fmt::print("Checking {}\n", fn);
    try {
        compat::check_type(fn).get();
    } catch (const compat::compat_error& e) {
        /*
         * boost.test seems to clip off exception messages that are really long.
         * so print it out separately and rethrow with a shorter message.
         */
        fmt::print("Check failed: {}", e.what());
        throw compat::compat_error(fn.string());
    }

    /*
     * Negative test -- modified input fails.
     */
    auto doc = compat::parse_type(fn).get();
    if (doc["fields"].MemberCount() == 0) {
        fmt::print("Test {} has no members. skipping negative test.\n", fn);
        return;
    }
    std::string perturb_path;
    auto changed = perturb(doc["fields"], doc, perturb_path);
    if (!changed.has_value()) {
        json::StringBuffer buf;
        json::PrettyWriter<json::StringBuffer> writer(buf);
        doc.Accept(writer);
        fmt::print("Test input: {}\n", buf.GetString());
        fmt::print(
          "Test {} has no changes possible. Skipping negative test\n", fn);
        return;
    }
    vassert(changed.value(), "Failed to perturb test case {}", fn);
    fmt::print("Negative check with change at path: {}\n", perturb_path);
    BOOST_REQUIRE_EXCEPTION(
      // NOLINTNEXTLINE(bugprone-use-after-move)
      compat::check_type(std::move(doc)).get(),
      compat::compat_error,
      [perturb_path](const compat::compat_error& e) {
          return std::string_view(e.what()).find("compat check failed for")
                 != std::string_view::npos;
      });
}

/*
 * process all the json files in the directory
 */
ss::future<> check_corpus(const std::filesystem::path& dir) {
    return directory_walker::walk(dir.string(), [dir](ss::directory_entry ent) {
        if (!ent.type || *ent.type != ss::directory_entry_type::regular) {
            return ss::now();
        }
        if (ent.name.find(".json") == ss::sstring::npos) {
            return ss::now();
        }
        if (ent.name == "compile_commands.json") {
            return ss::now();
        }
        auto fn = dir / ent.name.c_str();
        return ss::async(
          [fn = std::move(fn)]() mutable { check(std::move(fn)); });
    });
}
} // namespace

SEASTAR_THREAD_TEST_CASE(compat_self_test) {
    temporary_dir corpus("compat_self_test");
    compat::write_corpus(corpus.get_path()).get();
    check_corpus(corpus.get_path()).get();
}
