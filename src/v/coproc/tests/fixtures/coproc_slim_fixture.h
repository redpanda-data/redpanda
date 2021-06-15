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
#include "coproc/tests/fixtures/coproc_fixture_iface.h"
#include "model/metadata.h"
#include "storage/api.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/sharded.hh>

#include <filesystem>
#include <set>
#include <variant>

/// This fixture sets up the mininum viable framework neccessary to test the
/// script_dispatcher without using the redpanda_thread_fixture as that starts
/// an entire application stack.
class coproc_slim_fixture : public coproc_fixture_iface {
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

    virtual ss::future<> enable_coprocessors(std::vector<deploy>) override;

    virtual ss::future<> disable_coprocessors(std::vector<uint64_t>) override;

    virtual ss::future<> setup(log_layout_map) override;

    virtual ss::future<> restart() override;

    virtual ss::future<model::offset>
    push(const model::ntp&, model::record_batch_reader) override;

    /// \brief Read records from storage::api up until 'limit' or 'time'
    /// starting at 'offset'
    virtual ss::future<std::optional<model::record_batch_reader::data_t>> drain(
      const model::ntp&,
      std::size_t,
      model::offset = model::offset(0),
      model::timeout_clock::time_point = model::timeout_clock::now()
                                         + std::chrono::seconds(5)) override;

    /// Query what shards contain an ntp with the given topic
    //
    /// Useful for testing to know what stateful effect setup had. This can then
    /// be compared or used for verification during the test.
    ss::future<std::set<ss::shard_id>> shards_for_topic(const model::topic&);

    ss::future<size_t> scripts_across_shards(uint64_t id) {
        return get_pacemaker().map_reduce0(
          [id](coproc::pacemaker& p) {
              return p.local_script_id_exists(coproc::script_id(id)) ? 1 : 0;
          },
          size_t(0),
          std::plus<>());
    }

protected:
    std::unique_ptr<coproc::wasm::script_dispatcher>& get_script_dispatcher() {
        return _script_dispatcher;
    }
    ss::sharded<coproc::pacemaker>& get_pacemaker() { return _pacemaker; }

private:
    using request_type = std::
      variant<coproc::enable_copros_request, coproc::disable_copros_request>;

    ss::future<> start();
    ss::future<> stop();
    ss::future<> add_ntps(const model::topic&, size_t);

    storage::kvstore_config kvstore_config() const;
    storage::log_config log_config() const;

private:
    /// Cache the inital state of the system so it can be artifically rebuild on
    /// restart
    log_layout_map _llm;
    /// Cache all wasm deploy/remove requests, this emmulates the event_listener
    /// starting from the coproc topic at offset 0 when it is determined that a
    /// restart must occur
    std::vector<request_type> _cached_requests;

    std::filesystem::path _data_dir{
      std::filesystem::current_path() / "coproc_test"};
    ss::sharded<storage::api> _storage;
    ss::sharded<coproc::pacemaker> _pacemaker;
    ss::abort_source _abort_src;
    std::unique_ptr<coproc::wasm::script_dispatcher> _script_dispatcher;
};
