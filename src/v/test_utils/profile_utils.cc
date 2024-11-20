/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "test_utils/profile_utils.h"

#include "container/lw_shared_container.h"
#include "redpanda/admin/api-doc/debug.json.hh"
#include "resource_mgmt/cpu_profiler.h"
#include "resource_mgmt/memory_sampling.h"

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/sharded.hh>
#include <seastar/http/httpd.hh>
#include <seastar/json/json_elements.hh>
#include <seastar/util/log.hh>

#include <functional>
#include <limits>
#include <mutex>
#include <optional>

namespace test_utils {

class profile_helper {
public:
    explicit profile_helper(
      std::chrono::milliseconds cpu_profiler_sample_period)
      : _sample_period(cpu_profiler_sample_period) {}

    ss::future<> start();
    ss::future<> stop();

    ss::future<> write_cpu_profiler_results(std::filesystem::path path);
    ss::future<> write_memory_profiler_results(std::filesystem::path path);

private:
    std::chrono::milliseconds _sample_period;
    ss::sharded<resources::cpu_profiler> _cpu_profiler;
    ss::sharded<memory_sampling> _memory_sampler;
    seastar::logger _logger{"profile_helper_logger"};
};

ss::future<> profile_helper::start() {
    co_await _cpu_profiler.start(
      config::mock_binding(true),
      config::mock_binding<std::chrono::milliseconds>(_sample_period));
    co_await _cpu_profiler.invoke_on_all(&resources::cpu_profiler::start);
    co_await _memory_sampler.start(
      std::ref(_logger), config::mock_binding<bool>(true));
    co_await _memory_sampler.invoke_on_all(&memory_sampling::start);
}

ss::future<> profile_helper::stop() {
    co_await _cpu_profiler.stop();
    co_await _memory_sampler.stop();
}

namespace {
ss::future<ss::file> make_handle(
  std::filesystem::path path, ss::open_flags flags, ss::file_open_options opt) {
    auto file = co_await ss::open_file_dma(path.string(), flags, opt);

    co_return std::move(file);
}

ss::future<> write_to_file(
  std::filesystem::path path, seastar::json::json_return_type json) {
    auto handle = co_await make_handle(
      std::move(path), ss::open_flags::create | ss::open_flags::wo, {});
    auto ostream = co_await ss::make_file_output_stream(std::move(handle));
    if (json._body_writer) {
        co_await json._body_writer(std::move(ostream));
    } else {
        co_await ostream.write(json._res);
        co_await ostream.close();
    }
}
} // namespace

ss::future<>
profile_helper::write_cpu_profiler_results(std::filesystem::path path) {
    std::vector<resources::cpu_profiler::shard_samples> profiles
      = co_await _cpu_profiler.local().results(std::nullopt);

    auto json_res = co_await ss::make_ready_future<ss::json::json_return_type>(
      ss::json::stream_range_as_array(
        lw_shared_container(std::move(profiles)),
        [](const resources::cpu_profiler::shard_samples& profile) {
            ss::httpd::debug_json::cpu_profile_shard_samples ret;
            ret.shard_id = profile.shard;
            ret.dropped_samples = profile.dropped_samples;

            for (auto& sample : profile.samples) {
                ss::httpd::debug_json::cpu_profile_sample s;
                s.occurrences = sample.occurrences;
                s.user_backtrace = sample.user_backtrace;

                ret.samples.push(s);
            }
            return ret;
        }));

    co_await write_to_file(std::move(path), std::move(json_res));
}

ss::future<>
profile_helper::write_memory_profiler_results(std::filesystem::path path) {
    auto profiles = co_await _memory_sampler.local()
                      .get_sampled_memory_profiles(std::nullopt);

    std::vector<ss::httpd::debug_json::memory_profile> resp(profiles.size());
    for (size_t i = 0; i < resp.size(); ++i) {
        resp[i].shard = profiles[i].shard_id;

        for (auto& allocation_sites : profiles[i].allocation_sites) {
            ss::httpd::debug_json::allocation_site allocation_site;
            allocation_site.size = allocation_sites.size;
            allocation_site.count = allocation_sites.count;
            allocation_site.backtrace = std::move(allocation_sites.backtrace);
            resp[i].allocation_sites.push(allocation_site);
        }
    }

    co_await write_to_file(std::move(path), resp);
}

namespace {
constexpr auto cpu_profile_file_template = "{}_cpu_profile_{}.json";
constexpr auto memory_profile_file_template = "{}_memory_profile_{}.json";

std::string fmt_cpu_prof(std::string_view section_name, size_t id) {
    return std::format(cpu_profile_file_template, section_name, id);
}

std::string fmt_mem_prof(std::string_view section_name, size_t id) {
    return std::format(memory_profile_file_template, section_name, id);
}

ss::future<bool> profiles_exists_for(
  std::string_view section_name,
  std::filesystem::path path,
  size_t profile_id) {
    auto cpu_p_exists = co_await ss::file_exists(
      (path / fmt_cpu_prof(section_name, profile_id)).string());
    auto memory_p_exists = co_await ss::file_exists(
      (path / fmt_mem_prof(section_name, profile_id)).string());
    co_return cpu_p_exists || memory_p_exists;
}

ss::future<size_t> next_free_profile_file_id(
  std::string_view section_name, std::filesystem::path path) {
    for (size_t i = 0; i < std::numeric_limits<size_t>::max(); i++) {
        auto profiles_exist = co_await profiles_exists_for(
          section_name, path, i);
        if (!profiles_exist) {
            co_return i;
        }
    }

    co_return 0;
}
} // namespace

ss::future<> profile_section(
  std::string_view section_name,
  std::function<ss::future<>()> section,
  profile_options opts) {
    static std::mutex mux{};
    bool skip_profiles = false;

    std::unique_lock<std::mutex> lock(mux, std::defer_lock);
    if (!lock.try_lock()) {
        // If some other section is concurrently being profiled then skip
        // profiling this section.
        co_await section();
        co_return;
    }

    if (opts.once) {
        skip_profiles = co_await profiles_exists_for(
          section_name, opts.output_dir, 0);
    }

    if (skip_profiles) {
        co_await section();
        co_return;
    }

    auto profile_id = co_await next_free_profile_file_id(
      section_name, opts.output_dir);
    auto cpu_profile_path = opts.output_dir
                            / fmt_cpu_prof(section_name, profile_id);
    auto memory_profile_path = opts.output_dir
                               / fmt_mem_prof(section_name, profile_id);

    profile_helper ph(opts.profile_sample_period);
    co_await ph.start();

    co_await section();

    co_await ph.write_cpu_profiler_results(cpu_profile_path);
    co_await ph.write_memory_profiler_results(memory_profile_path);
    co_await ph.stop();
}

} // namespace test_utils
