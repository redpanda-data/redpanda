#pragma once
#include "coproc/router.h"
#include "coproc/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record_batch_reader.h"
#include "random/generators.h"
#include "storage/api.h"

#include <seastar/core/future.hh>

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

using log_layout_map = absl::flat_hash_map<model::topic_namespace, size_t>;
using active_copros = std::vector<coproc::enable_copros_request::data>;

class coproc_test_fixture {
public:
    using opt_reader_data_t = std::optional<model::record_batch_reader::data_t>;
    using erc = coproc::enable_response_code;
    using drc = coproc::disable_response_code;

    static const inline auto e = coproc::topic_ingestion_policy::earliest;
    static const inline auto s = coproc::topic_ingestion_policy::stored;
    static const inline auto l = coproc::topic_ingestion_policy::latest;

    explicit coproc_test_fixture(bool start_router)
      : _start_router(start_router) {}

    coproc_test_fixture(coproc_test_fixture&) = delete;
    coproc_test_fixture(coproc_test_fixture&&) = delete;
    coproc_test_fixture& operator=(coproc_test_fixture&) = delete;
    coproc_test_fixture& operator=(coproc_test_fixture&&) = delete;

    virtual ~coproc_test_fixture();

    /// \brief Write records to storage::api
    ss::future<>
    push(const model::ntp&, ss::circular_buffer<model::record_batch>&&);

    /// \brief Read records from storage::api up until 'limit' or 'time'
    ss::future<opt_reader_data_t>
    drain(const model::ntp&, std::size_t, model::timeout_clock::time_point);

    // Accessors & getters for test internals
    const absl::flat_hash_set<model::ntp>& get_data() const { return _data; }
    absl::flat_hash_set<model::ntp>& get_data() { return _data; }

    const ss::sharded<storage::api>& get_api() const { return _api; }
    ss::sharded<storage::api>& get_api() { return _api; }

    const ss::sharded<coproc::router>& get_router() const { return _router; }
    ss::sharded<coproc::router>& get_router() { return _router; }

protected:
    /// \brief Populate '_api' with the user defined test layout
    void startup(log_layout_map&& data, active_copros&& = {});

    /// \brief Populate '_router' with the user defined test layout
    ss::future<> expand_copros(active_copros&&);

private:
    ss::future<> expand();

    ss::future<> maybe_create_log(storage::api& api, const model::ntp&);

    ss::future<model::record_batch_reader::data_t>
    do_drain(storage::log&&, std::size_t, model::timeout_clock::time_point);

    /// \brief For these tests all ntps are hashed to core in this fashion.
    static ss::shard_id hash_scheme(const model::topic& topic) {
        // NOTE: in prod, this won't be the case
        if (auto mt = model::make_materialized_topic(topic)) {
            return std::hash<model::topic>()(mt->src) % ss::smp::count;
        }
        return std::hash<model::topic>()(topic) % ss::smp::count;
    }

    storage::kvstore_config default_kvstorecfg() {
        return storage::kvstore_config(
          8192,
          std::chrono::milliseconds(10),
          _cfg_dir,
          storage::debug_sanitize_files::yes);
    }

    storage::log_config default_logcfg() {
        return storage::log_config(
          storage::log_config::storage_type::disk,
          _cfg_dir,
          100_MiB,
          storage::debug_sanitize_files::yes);
    }

private:
    bool _start_router;
    absl::flat_hash_set<model::ntp> _data;
    ss::sharded<storage::api> _api;
    ss::sharded<coproc::router> _router;
    ss::sstring _cfg_dir{
      "test_directory/" + random_generators::gen_alphanum_string(4)};
};
