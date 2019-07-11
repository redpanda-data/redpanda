#include "raft_client_cache.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh> // timer::arm/re-arm here
#include <seastar/core/sleep.hh>
#include <seastar/core/timer.hh>

#include <smf/log.h>

#include <fmt/format.h>

#include <chrono>

// clang-format off
int main(int args, char** argv, char** env) {
    std::cout.setf(std::ios::unitbuf);
    LOG_INFO("sizeof tagged ptr {}", sizeof(tagged_ptr<raft::raft_api_client>));
    app_template app;
    std::vector<ipv4_addr> addrs;
    return app.run(args, argv, [&] {
        smf::app_run_log_level(log_level::trace);
        for (auto i = 0; i < 10; ++i) {
            addrs.emplace_back(fmt::format("127.0.0.1:1234{}", i));
        }
        return do_with(raft_client_cache(), [&addrs](auto& cache) {
            return parallel_for_each(
              addrs.begin(),
              addrs.end(),
              [&cache](auto& addr) {
                  return cache.get_connection(addr).discard_result();
              })
              .then([] { return sleep(std::chrono::seconds(1)); })
              .then([&cache] { return cache.close(); });
        }).then([] { return make_ready_future<int>(0); });
    });
}
// clang-format on
