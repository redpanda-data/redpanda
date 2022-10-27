/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iobuf.h"
#include "bytes/iostream.h"
#include "cloud_storage/access_time_tracker.h"
#include "cloud_storage/recursive_directory_walker.h"
#include "seastarx.h"
#include "test_utils/tmp_dir.h"

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/file.hh>
#include <seastar/util/tmp_file.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <chrono>
#include <set>

using namespace cloud_storage;
using namespace std::chrono_literals;

std::set<std::string> result_paths(cloud_storage::walk_result& r) {
    std::set<std::string> paths;
    for (const auto& f : r.regular_files) {
        paths.insert(f.path);
    }
    return paths;
}

SEASTAR_THREAD_TEST_CASE(one_level) {
    temporary_dir tmpdir("directory-walker");
    cloud_storage::recursive_directory_walker _walker;
    const std::filesystem::path target_dir = tmpdir.get_path();
    const std::filesystem::path file_path1 = target_dir / "file1.txt";
    const std::filesystem::path file_path2 = target_dir / "file2.txt";

    auto flags = ss::open_flags::wo | ss::open_flags::create
                 | ss::open_flags::exclusive;
    auto file1 = ss::open_file_dma(file_path1.native(), flags).get();
    file1.close().get();

    // sleep is needed to ensure stable order of returned files. directory
    // walker returns files sorted by timestamp of creation, xfs stores
    // creation time with 1 second precision.
    ss::sleep(ss::lowres_clock::duration(1s)).get();

    auto file2 = ss::open_file_dma(file_path2.native(), flags).get();
    file2.close().get();

    access_time_tracker tracker;
    auto result = _walker.walk(target_dir.native(), tracker).get();

    auto expect = std::set<std::string>{
      file_path1.native(), file_path2.native()};
    BOOST_REQUIRE_EQUAL(result.cache_size, 0);
    BOOST_REQUIRE(result_paths(result) == expect);
}

SEASTAR_THREAD_TEST_CASE(three_levels) {
    temporary_dir tmpdir("directory-walker");
    cloud_storage::recursive_directory_walker _walker;
    const std::filesystem::path target_dir = tmpdir.get_path();
    const std::filesystem::path file_path1 = target_dir / "a" / "file1.txt";
    const std::filesystem::path file_path2 = target_dir / "file2.txt";
    const std::filesystem::path file_path3 = target_dir / "b" / "c"
                                             / "file3.txt";

    ss::recursive_touch_directory((target_dir / "a").native()).get();
    ss::recursive_touch_directory((target_dir / "b" / "c").native()).get();

    auto flags = ss::open_flags::wo | ss::open_flags::create
                 | ss::open_flags::exclusive;
    auto file1 = ss::open_file_dma(file_path1.native(), flags).get();
    file1.close().get();

    auto file2 = ss::open_file_dma(file_path2.native(), flags).get();
    file2.close().get();

    auto file3 = ss::open_file_dma(file_path3.native(), flags).get();
    file3.close().get();

    access_time_tracker tracker;
    auto result = _walker.walk(target_dir.native(), tracker).get();

    BOOST_REQUIRE_EQUAL(result.cache_size, 0);
    BOOST_REQUIRE_EQUAL(result.regular_files.size(), 3);

    auto expect = std::set<std::string>{
      file_path1.native(), file_path2.native(), file_path3.native()};
    BOOST_REQUIRE(result_paths(result) == expect);
}

SEASTAR_THREAD_TEST_CASE(no_files) {
    temporary_dir tmpdir("directory-walker");
    cloud_storage::recursive_directory_walker _walker;
    const std::filesystem::path target_dir = tmpdir.get_path();
    const std::filesystem::path dir1 = target_dir / "a" / "b";
    const std::filesystem::path dir2 = target_dir / "c";

    ss::recursive_touch_directory(dir1.native()).get();
    ss::recursive_touch_directory(dir2.native()).get();

    access_time_tracker tracker;
    auto result = _walker.walk(target_dir.native(), tracker).get();

    BOOST_REQUIRE_EQUAL(result.cache_size, 0);
    BOOST_REQUIRE_EQUAL(result.regular_files.size(), 0);
}

SEASTAR_THREAD_TEST_CASE(empty_dir) {
    temporary_dir tmpdir("directory-walker");
    cloud_storage::recursive_directory_walker _walker;
    const std::filesystem::path target_dir = tmpdir.get_path();

    access_time_tracker tracker;
    auto result = _walker.walk(target_dir.native(), tracker).get();

    BOOST_REQUIRE_EQUAL(result.cache_size, 0);
    BOOST_REQUIRE_EQUAL(result.regular_files.size(), 0);
}

void write_to_file(auto& target_file, uint64_t size) {
    ss::sstring data_string;
    data_string.resize(size, 'a');

    iobuf buf;
    buf.append(data_string.data(), data_string.length());

    auto input = make_iobuf_input_stream(std::move(buf));
    auto out = ss::make_file_output_stream(target_file).get();

    ss::copy(input, out).get();
    out.flush().get();
    out.close().get();
}

SEASTAR_THREAD_TEST_CASE(total_size_correct) {
    temporary_dir tmpdir("directory-walker");
    cloud_storage::recursive_directory_walker _walker;
    const std::filesystem::path target_dir = tmpdir.get_path();
    const std::filesystem::path file_path1 = target_dir / "a" / "file1.txt";
    const std::filesystem::path file_path2 = target_dir / "file2.txt";
    const std::filesystem::path file_path3 = target_dir / "b" / "c"
                                             / "file3.txt";

    ss::recursive_touch_directory((target_dir / "a").native()).get();
    ss::recursive_touch_directory((target_dir / "b" / "c").native()).get();

    auto flags = ss::open_flags::wo | ss::open_flags::create
                 | ss::open_flags::exclusive;
    auto file1 = ss::open_file_dma(file_path1.native(), flags).get();
    auto file2 = ss::open_file_dma(file_path2.native(), flags).get();
    auto file3 = ss::open_file_dma(file_path3.native(), flags).get();

    write_to_file(file1, 3412);
    write_to_file(file2, 8);
    write_to_file(file3, 342);

    access_time_tracker tracker;
    auto result = _walker.walk(target_dir.native(), tracker).get();

    BOOST_REQUIRE_EQUAL(result.cache_size, 3412 + 8 + 342);
    BOOST_REQUIRE_EQUAL(result.regular_files.size(), 3);
}
