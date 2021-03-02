/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "coproc/pacemaker.h"
#include "coproc/script_dispatcher.h"
#include "coproc/tests/utils/coproc_test_fixture.h"
#include "model/metadata.h"
#include "storage/api.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/sharded.hh>

#include <filesystem>
#include <set>

/// This fixture sets up the mininum viable framework neccessary to test the
/// script_dispatcher without using the redpanda_thread_fixture as that starts
/// an entire application stack.
class coproc_slim_fixture {
public:
    /// Class constructor, sets up the data_dir
    coproc_slim_fixture();

    /// Disabling move & copy assignment
    coproc_slim_fixture(const coproc_slim_fixture&) = delete;
    coproc_slim_fixture(coproc_slim_fixture&&) = delete;
    coproc_slim_fixture& operator=(const coproc_slim_fixture&) = delete;
    coproc_slim_fixture& operator=(coproc_slim_fixture&&) = delete;

    /// Class destructor, destroys resources in order, and rm's data_dir
    ~coproc_slim_fixture();

    /// Call to define the state-of-the-world
    ///
    /// This is the current ntps registered within the log manger. Since theres
    /// no shard_table, a trivial hashing scheme is internally used to map ntps
    /// to shards.
    ss::future<> setup(log_layout_map);

    /// Query what shards contain an ntp with the given topic
    //
    /// Useful for testing to know what stateful effect setup had. This can then
    /// be compared or used for verification during the test.
    ss::future<std::set<ss::shard_id>> shards_for_topic(const model::topic&);

protected:
    std::unique_ptr<coproc::wasm::script_dispatcher>& get_script_dispatcher() {
        return _script_dispatcher;
    }
    ss::sharded<coproc::pacemaker>& get_pacemaker() { return _pacemaker; }

private:
    ss::future<> start();
    ss::future<> stop();
    ss::future<> add_ntps(const model::topic&, size_t);

    storage::kvstore_config kvstore_config() const;
    storage::log_config log_config() const;

private:
    std::filesystem::path _data_dir{
      std::filesystem::current_path() / "coproc_test"};
    ss::sharded<storage::api> _storage;
    ss::sharded<coproc::pacemaker> _pacemaker;
    ss::abort_source _abort_src;
    std::unique_ptr<coproc::wasm::script_dispatcher> _script_dispatcher;
};
