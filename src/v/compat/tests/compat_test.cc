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
#include "compat/run.h"
#include "seastarx.h"
#include "test_utils/tmp_dir.h"
#include "utils/directory_walker.h"
#include "utils/file_io.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

namespace {
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
        const auto fn = dir / ent.name.c_str();
        fmt::print("Checking {}\n", fn);
        return compat::check_type(fn);
    });
}
} // namespace

SEASTAR_THREAD_TEST_CASE(compat_self_test) {
    temporary_dir corpus("compat_self_test");
    compat::write_corpus(corpus.get_path()).get();
    check_corpus(corpus.get_path()).get();
}
