/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iostream.h"
#include "cloud_storage/cache_service.h"
#include "cloud_storage/download_exception.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/remote_file.h"
#include "cloud_storage/tests/cache_test_fixture.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "test_utils/fixture.h"
#include "utils/lazy_abort_source.h"

#include <seastar/core/seastar.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/defer.hh>

#include <boost/test/unit_test.hpp>

namespace {

ss::abort_source never_abort;
lazy_abort_source always_continue{[]() { return std::nullopt; }};
constexpr model::cloud_credentials_source config_file{
  model::cloud_credentials_source::config_file};

iobuf make_iobuf_from_string(std::string_view s) {
    iobuf b;
    b.append(s.data(), s.size());
    return b;
}

} // namespace
using namespace cloud_storage;

class remote_file_fixture
  : public s3_imposter_fixture
  , public cache_test_fixture {
public:
    remote_file_fixture()
      : data_dir("data_dir") {
        ss::recursive_touch_directory(data_dir.get_path().string()).get();
        auto conf = get_configuration();
        pool
          .start(
            10, ss::sharded_parameter([this] { return get_configuration(); }))
          .get();
        io.start(
            std::ref(pool),
            ss::sharded_parameter([this] { return get_configuration(); }),
            ss::sharded_parameter([] { return config_file; }))
          .get();
        remote
          .start(std::ref(io), ss::sharded_parameter([this] {
                     return get_configuration();
                 }))
          .get();
        set_expectations_and_listen({});
    }

    std::filesystem::path test_path(const ss::sstring& name) {
        return std::filesystem::path{
          data_dir.get_path() / std::filesystem::path(name)};
    }

    void write_local_file(
      const std::filesystem::path& filepath, const ss::sstring& body) {
        ss::open_flags flags = ss::open_flags::rw | ss::open_flags::create;
        auto f = ss::open_file_dma(
                   filepath.string(), flags, ss::file_open_options{})
                   .get();

        ss::file_output_stream_options w_opts;
        auto os = ss::make_file_output_stream(std::move(f), w_opts).get();
        auto close = ss::defer([&os] { os.close().get(); });
        write_iobuf_to_output_stream(make_iobuf_from_string(body), os).get();
    }

    upload_result upload_local_file(
      const std::filesystem::path& file_path,
      const remote_segment_path& remote_path,
      retry_chain_node& retry_node) {
        ss::open_flags flags = ss::open_flags::ro;
        ss::file f = ss::open_file_dma(file_path.string(), flags).get();
        return remote.local()
          .upload_controller_snapshot(
            bucket_name, remote_path, f, retry_node, always_continue)
          .get();
    }

    ~remote_file_fixture() {
        data_dir.remove().get();
        pool.local().shutdown_connections();
        io.local().request_stop();
        remote.stop().get();
        io.stop().get();
        pool.stop().get();
    }

    temporary_dir data_dir;
    ss::sharded<cloud_storage_clients::client_pool> pool;
    ss::sharded<cloud_io::remote> io;
    ss::sharded<remote> remote;
};

FIXTURE_TEST(test_cached_file, remote_file_fixture) {
    const ss::sstring local_path = "file";
    const ss::sstring body = "body";
    const auto file_path = test_path(local_path);

    // Write a local file and then upload.
    write_local_file(file_path, body);
    const remote_segment_path remote_path{"remote_path"};
    retry_chain_node retry_node(never_abort, 10s, 20ms);
    auto upl_res = upload_local_file(file_path, remote_path, retry_node);
    BOOST_REQUIRE(upl_res == upload_result::success);
    BOOST_REQUIRE_EQUAL(1, get_requests().size());

    // Download the file and ensure it matches the expected body. Repeated
    // attempts should fetch from cache rather than from the remote.
    remote_file rfile(
      remote.local(),
      sharded_cache.local(),
      bucket_name,
      remote_path,
      retry_node,
      ss::sstring("log_prefix"));
    for (int i = 0; i < 3; i++) {
        auto hydrated_file = rfile.hydrate_readable_file().get();
        auto cached_size = hydrated_file.size().get();
        auto buf = hydrated_file.dma_read_bulk<char>(0, cached_size).get();
        auto cached_body = ss::to_sstring(std::move(buf));
        BOOST_REQUIRE_EQUAL(cached_body, body);
        BOOST_REQUIRE_EQUAL(2, get_requests().size());
    }

    // Now invalidate the cache and try again.
    sharded_cache.local()
      .invalidate(std::filesystem::path{remote_path()})
      .get();
    auto hydrated_file = rfile.hydrate_readable_file().get();
    auto cached_size = hydrated_file.size().get();
    auto buf = hydrated_file.dma_read_bulk<char>(0, cached_size).get();
    auto cached_body = ss::to_sstring(std::move(buf));
    BOOST_REQUIRE_EQUAL(cached_body, body);
    BOOST_REQUIRE_EQUAL(3, get_requests().size());
}

FIXTURE_TEST(test_missing_file, remote_file_fixture) {
    retry_chain_node retry_node(never_abort, 100ms, 20ms);
    const remote_segment_path remote_path{"remote_path"};
    remote_file rfile(
      remote.local(),
      sharded_cache.local(),
      bucket_name,
      remote_path,
      retry_node,
      ss::sstring("log_prefix"));
    BOOST_REQUIRE_EXCEPTION(
      rfile.hydrate_readable_file().get(), download_exception, [](auto& e) {
          ss::sstring what{e.what()};
          return what.find("NotFound") != what.npos;
      });
}
