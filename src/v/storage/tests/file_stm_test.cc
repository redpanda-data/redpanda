// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/file_stm.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/tmp_file.hh>

#include <boost/scope_exit.hpp>
#include <boost/test/tools/old/interface.hpp>

#include <chrono>

SEASTAR_THREAD_TEST_CASE(wait_list_void_trigger_all) {
    using namespace storage::details;
    wait_list<> list;
    auto fut1 = list.wait();
    auto fut2 = list.wait();
    BOOST_REQUIRE(!fut1.available());
    BOOST_REQUIRE(!fut2.available());
    list.trigger_all();
    BOOST_REQUIRE(fut1.available());
    BOOST_REQUIRE(fut2.available());
    fut1.get();
    fut2.get();
}

SEASTAR_THREAD_TEST_CASE(wait_list_int_trigger_all) {
    using namespace storage::details;
    wait_list<int> list;
    auto fut1 = list.wait();
    auto fut2 = list.wait();
    BOOST_REQUIRE(!fut1.available());
    BOOST_REQUIRE(!fut2.available());
    list.trigger_all(42);
    BOOST_REQUIRE(fut1.available());
    BOOST_REQUIRE(fut2.available());
    BOOST_REQUIRE_EQUAL(fut1.get0(), 42);
    BOOST_REQUIRE_EQUAL(fut2.get0(), 42);
}

SEASTAR_THREAD_TEST_CASE(wait_list_trigger_by_one) {
    using namespace storage::details;
    wait_list<int> list;
    auto fut1 = list.wait();
    auto fut2 = list.wait();
    BOOST_REQUIRE(!fut1.available());
    BOOST_REQUIRE(!fut2.available());
    list.trigger(42);
    BOOST_REQUIRE(fut1.available());
    BOOST_REQUIRE(!fut2.available());
    BOOST_REQUIRE_EQUAL(fut1.get0(), 42);
    list.trigger(24);
    BOOST_REQUIRE(fut2.available());
    BOOST_REQUIRE_EQUAL(fut2.get0(), 24);
}

SEASTAR_THREAD_TEST_CASE(wait_list_destroyed) {
    using namespace storage::details;
    auto fut = ss::now();
    {
        wait_list<> list;
        fut = list.wait();
    }
    BOOST_REQUIRE(fut.available());
    BOOST_REQUIRE(fut.failed());
    BOOST_REQUIRE_THROW(fut.get0(), ss::broken_promise);
}

SEASTAR_THREAD_TEST_CASE(wait_list_trigger_empty) {
    using namespace storage::details;
    wait_list<> list;
    list.trigger();
    BOOST_REQUIRE(list.size() == 0);
}

static ss::future<> no_op_event() { return ss::now(); }

SEASTAR_THREAD_TEST_CASE(open_file_cache_entry_dtor) {
    using namespace storage;
    open_file_cache cache(1);
    BOOST_REQUIRE(cache.empty());
    {
        open_file_cache::entry_t node{
          ._on_evict = &no_op_event,
        };
        BOOST_REQUIRE(!node._hook.is_linked());
        cache.put(node).get();
        BOOST_REQUIRE(!cache.empty());
        BOOST_REQUIRE(node._hook.is_linked());
    }
    BOOST_REQUIRE(cache.empty());
}

SEASTAR_THREAD_TEST_CASE(open_file_cache_put_evict) {
    using namespace storage;
    open_file_cache cache(1);
    BOOST_REQUIRE(cache.empty());
    open_file_cache::entry_t node{
      ._on_evict = &no_op_event,
    };
    BOOST_REQUIRE(!node._hook.is_linked());
    cache.put(node).get();
    BOOST_REQUIRE(node._hook.is_linked());
    cache.evict(node);
    BOOST_REQUIRE(!node._hook.is_linked());
}

SEASTAR_THREAD_TEST_CASE(open_file_cache_no_wait_evict) {
    using namespace storage;
    open_file_cache cache(1);
    BOOST_REQUIRE(cache.empty());
    bool node1_triggered = false;
    open_file_cache::entry_t node1{
      ._on_evict =
        [&node1_triggered] {
            node1_triggered = true;
            return ss::now();
        },
    };
    open_file_cache::entry_t node2{
      ._on_evict = &no_op_event,
    };
    cache.put(node1).get();
    BOOST_REQUIRE(cache.is_full());
    auto fut = cache.put(node2); // this future should evict node1
    BOOST_REQUIRE(fut.available());
    fut.get();
    BOOST_REQUIRE(node1_triggered);
    BOOST_REQUIRE(cache.is_full());
    cache.evict(node1);
    BOOST_REQUIRE(!node1._hook.is_linked());
    BOOST_REQUIRE(node2._hook.is_linked());
}

SEASTAR_THREAD_TEST_CASE(open_file_cache_wait_evict) {
    using namespace storage;
    open_file_cache cache(1);
    BOOST_REQUIRE(cache.empty());
    bool node1_triggered = false;
    open_file_cache::entry_t node1{
      ._on_evict =
        [&node1_triggered] {
            node1_triggered = true;
            return ss::now();
        },
    };
    open_file_cache::entry_t node2{
      ._on_evict = &no_op_event,
    };
    cache.put(node1).get();
    BOOST_REQUIRE(cache.is_full());
    BOOST_REQUIRE(cache.is_candidate(node1));
    node1.gate.enter(); // this should prevent subsequent put to evict node1
    auto fut = cache.put(node2); // this future should evict node1
    BOOST_REQUIRE(cache.is_candidate(node1));
    BOOST_REQUIRE(
      !fut.available()); // will only be available after node1 eviction
    node1.gate.leave();
    cache.evict(node1);
    fut.get();
    BOOST_REQUIRE(node1_triggered);
    BOOST_REQUIRE(cache.is_full());
    BOOST_REQUIRE(!node1._hook.is_linked());
    BOOST_REQUIRE(node2._hook.is_linked());
}

SEASTAR_THREAD_TEST_CASE(file_stm_close) {
    ss::tmp_dir dir = ss::make_tmp_dir("/tmp/file-stm-test-XXXX").get0();
    BOOST_SCOPE_EXIT_ALL(&dir) { dir.remove().get(); };
    using namespace storage;
    auto cache = ss::make_lw_shared<open_file_cache>(3);
    auto path1 = dir.get_path();
    path1.append("file1");
    auto path2 = dir.get_path();
    path2.append("file2");
    auto file1 = open_file_dma(
                   path1.native(),
                   ss::open_flags::create | ss::open_flags::rw,
                   cache)
                   .get0();
    auto file2 = open_file_dma(
                   path2.native(),
                   ss::open_flags::create | ss::open_flags::rw,
                   cache)
                   .get0();
    file1.close().get();
    file2.close().get();
}

SEASTAR_THREAD_TEST_CASE(file_stm_evict) {
    ss::tmp_dir dir = ss::make_tmp_dir("/tmp/file-stm-test-XXXX").get0();
    BOOST_SCOPE_EXIT_ALL(&dir) { dir.remove().get(); };
    using namespace storage;
    auto cache = ss::make_lw_shared<open_file_cache>(3);
    auto path1 = dir.get_path();
    path1.append("file1");
    auto path2 = dir.get_path();
    path2.append("file2");
    auto path3 = dir.get_path();
    path3.append("file3");
    auto file1 = open_file_dma(
                   path1.native(),
                   ss::open_flags::create | ss::open_flags::rw,
                   cache)
                   .get0();
    auto file2 = open_file_dma(
                   path2.native(),
                   ss::open_flags::create | ss::open_flags::rw,
                   cache)
                   .get0();
    auto file3 = open_file_dma(
                   path3.native(),
                   ss::open_flags::create | ss::open_flags::rw,
                   cache)
                   .get0();
    BOOST_REQUIRE(file1.is_evicted());
    BOOST_REQUIRE(file2.is_opened());
    BOOST_REQUIRE(file3.is_opened());
    file1.close().get();
    file2.close().get();
    file3.close().get();
}

SEASTAR_THREAD_TEST_CASE(file_stm_reopen_triggered_by_access) {
    ss::tmp_dir dir = ss::make_tmp_dir("/tmp/file-stm-test-XXXX").get0();
    BOOST_SCOPE_EXIT_ALL(&dir) { dir.remove().get(); };
    using namespace storage;
    auto cache = ss::make_lw_shared<open_file_cache>(3);
    auto path1 = dir.get_path();
    path1.append("file1");
    auto path2 = dir.get_path();
    path2.append("file2");
    auto path3 = dir.get_path();
    path3.append("file3");
    auto file1 = open_file_dma(
                   path1.native(),
                   ss::open_flags::create | ss::open_flags::rw,
                   cache)
                   .get0();
    auto file2 = open_file_dma(
                   path2.native(),
                   ss::open_flags::create | ss::open_flags::rw,
                   cache)
                   .get0();
    auto file3 = open_file_dma(
                   path3.native(),
                   ss::open_flags::create | ss::open_flags::rw,
                   cache)
                   .get0();
    BOOST_REQUIRE(file1.is_evicted());
    BOOST_REQUIRE(file2.is_opened());
    BOOST_REQUIRE(file3.is_opened());
    file1
      .with_file([file1, file2, file3](const ss::file& underlying) {
          BOOST_REQUIRE(static_cast<bool>(underlying));
          BOOST_REQUIRE(file1.is_opened());
          BOOST_REQUIRE(file2.is_evicted());
          BOOST_REQUIRE(file3.is_opened());
          return ss::now();
      })
      .get();
    file1.close().get();
    file2.close().get();
    file3.close().get();
}

SEASTAR_THREAD_TEST_CASE(file_stm_blocked_eviction) {
    /// File open operation can't evict another file immediately because it's in
    /// use
    ss::tmp_dir dir = ss::make_tmp_dir("/tmp/file-stm-test-XXXX").get0();
    BOOST_SCOPE_EXIT_ALL(&dir) { dir.remove().get(); };
    using namespace storage;
    auto cache = ss::make_lw_shared<open_file_cache>(2);
    auto path1 = dir.get_path();
    path1.append("file1");
    auto path2 = dir.get_path();
    path2.append("file2");
    auto file1 = open_file_dma(
                   path1.native(),
                   ss::open_flags::create | ss::open_flags::rw,
                   cache)
                   .get0();
    auto file2 = open_file_dma(
                   path2.native(),
                   ss::open_flags::create | ss::open_flags::rw,
                   cache,
                   open_file_stm_options::lazy)
                   .get0();
    BOOST_REQUIRE(file1.is_opened());
    BOOST_REQUIRE(
      file2.is_pre_open()); // file is to be opened on first access attempt
    file1
      .with_file([file1, file2](const ss::file& underlying) mutable {
          // file1 is an eviction candidate because it's LRU, it can't be
          // evicted while this lambda is still running
          BOOST_REQUIRE(static_cast<bool>(underlying));
          BOOST_REQUIRE(file1.is_opened());
          BOOST_REQUIRE(file2.is_pre_open());
          return file2.with_file(
            [file1, file2](const ss::file& underlying) mutable {
                // here first lambda is not running anymore, file1 should be
                // evicted
                BOOST_REQUIRE(static_cast<bool>(underlying));
                BOOST_REQUIRE(file1.is_evicted());
                BOOST_REQUIRE(file2.is_opened());
                return file1.with_file(
                  [file1, file2](const ss::file& underlying) mutable {
                      BOOST_REQUIRE(static_cast<bool>(underlying));
                      BOOST_REQUIRE(file1.is_opened());
                      BOOST_REQUIRE(file2.is_evicted());
                      return ss::now();
                  });
            });
      })
      .get();
    file1.close().get();
    file2.close().get();
}

SEASTAR_THREAD_TEST_CASE(file_stm_eviction_triggered_by_close) {
    // Eviction is triggered by close of the file.
    ss::tmp_dir dir = ss::make_tmp_dir("/tmp/file-stm-test-XXXX").get0();
    BOOST_SCOPE_EXIT_ALL(&dir) { dir.remove().get(); };
    using namespace storage;
    auto cache = ss::make_lw_shared<open_file_cache>(3);
    auto path1 = dir.get_path();
    path1.append("file1");
    auto path2 = dir.get_path();
    path2.append("file2");
    auto path3 = dir.get_path();
    path3.append("file3");
    auto file1 = open_file_dma(
                   path1.native(),
                   ss::open_flags::create | ss::open_flags::rw,
                   cache)
                   .get0();
    auto file2 = open_file_dma(
                   path2.native(),
                   ss::open_flags::create | ss::open_flags::rw,
                   cache)
                   .get0();
    auto file3 = open_file_dma(
                   path3.native(),
                   ss::open_flags::create | ss::open_flags::rw,
                   cache,
                   open_file_stm_options::lazy)
                   .get0();
    BOOST_REQUIRE(file1.is_opened());
    BOOST_REQUIRE(file2.is_opened());
    BOOST_REQUIRE(
      file3.is_pre_open()); // file is to be opened on first access attempt
    file1
      .with_file([file1, file2, file3](const ss::file& underlying) mutable {
          BOOST_REQUIRE(static_cast<bool>(underlying));
          BOOST_REQUIRE(file1.is_opened());
          BOOST_REQUIRE(file2.is_opened());
          BOOST_REQUIRE(file3.is_pre_open());
          auto fut = file3.with_file(
            [file1, file2, file3](const ss::file& underlying) mutable {
                // this will wait until file1 will be closed
                BOOST_REQUIRE(static_cast<bool>(underlying));
                BOOST_REQUIRE(file1.is_opened());
                BOOST_REQUIRE(file2.is_closed());
                BOOST_REQUIRE(file3.is_opened());
                return ss::now();
            });
          // should become available when file1 will be closed
          return ss::when_all_succeed(file2.close(), std::move(fut));
      })
      .get();
    file1.close().get();
    file3.close().get();
}
