// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "random/generators.h"
#include "utils/directory_walker.h"

#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/testing/thread_test_case.hh>

#include <fmt/format.h>

SEASTAR_THREAD_TEST_CASE(empty_dir) {
    auto dir = "test.dir_" + random_generators::gen_alphanum_string(4);
    ss::recursive_touch_directory(dir).get();

    int count = 0;
    directory_walker::walk(dir, [&count](ss::directory_entry de) mutable {
        count++;
        return ss::make_ready_future<>();
    }).get();

    BOOST_CHECK_EQUAL(count, 0);
}

SEASTAR_THREAD_TEST_CASE(non_empty_dir) {
    auto dir = "test.dir_" + random_generators::gen_alphanum_string(4);
    ss::recursive_touch_directory(dir).get();

    // sees directories
    int count = 0;
    ss::recursive_touch_directory(dir + "/dir0").get();
    directory_walker::walk(dir, [&count](ss::directory_entry de) mutable {
        count++;
        return ss::make_ready_future<>();
    }).get();
    BOOST_CHECK_EQUAL(count, 1);

    // sees normal files
    count = 0;
    ss::open_file_dma(
      dir + "/file0", ss::open_flags::ro | ss::open_flags::create)
      .discard_result()
      .get();
    directory_walker::walk(dir, [&count](ss::directory_entry de) mutable {
        count++;
        return ss::make_ready_future<>();
    }).get();
    BOOST_CHECK_EQUAL(count, 2);

    // sees links
    count = 0;
    ss::link_file(dir + "/file0", dir + "/file1").get();
    directory_walker::walk(dir, [&count](ss::directory_entry de) mutable {
        count++;
        return ss::make_ready_future<>();
    }).get();
    BOOST_CHECK_EQUAL(count, 3);
}

SEASTAR_THREAD_TEST_CASE(exceptional_future) {
    auto dir = "test.dir_" + random_generators::gen_alphanum_string(4);
    ss::recursive_touch_directory(dir).get();

    // make sure we have some files in the directory
    int count = 0;
    for (int i = 0; i < 3; i++) {
        ss::open_file_dma(
          fmt::format("{}/file{}", dir, i),
          ss::open_flags::ro | ss::open_flags::create)
          .discard_result()
          .get();
    }
    auto f = directory_walker::walk(
      dir, [&count](ss::directory_entry de) mutable {
          count++;
          return ss::make_ready_future<>();
      });
    f.wait();
    BOOST_REQUIRE(!f.failed());
    f.ignore_ready_future();
    BOOST_REQUIRE_EQUAL(count, 3);

    // in the middle of the traversal we return an exceptional future. the walk
    // should stop early and the returned future should be failed.
    count = 0;
    auto f2 = directory_walker::walk(
      dir, [&count](ss::directory_entry de) mutable {
          count++;
          if (count == 2) {
              return ss::make_exception_future<>(std::runtime_error("foo"));
          }
          return ss::make_ready_future<>();
      });
    f2.wait();
    BOOST_CHECK(f2.failed());
    f2.ignore_ready_future();
    BOOST_REQUIRE_EQUAL(count, 2);
}

SEASTAR_THREAD_TEST_CASE(test_empty_dir) {
    auto dir = std::filesystem::path(
      "test.dir_" + random_generators::gen_alphanum_string(4));
    ss::recursive_touch_directory(dir.string()).get();
    BOOST_REQUIRE(directory_walker::empty(dir).get0());
    ss::recursive_touch_directory((dir / "xxx").string()).get();
    BOOST_REQUIRE(!directory_walker::empty(dir).get0());
}
