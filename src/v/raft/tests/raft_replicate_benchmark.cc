#include "raft/tests/raft_fixture_base.h"

#include <seastar/testing/perf_tests.hh>
namespace raft {
enum class stage {
    enqueue,
    replicate,
};
struct raft_bench_parameters {
    stage stage;
    raft::consistency_level c_lvl;
    size_t batch_size;
    size_t batch_count;
};

struct raft_benchmark_fixture {
public:
    ss::future<int> run_test(const raft_bench_parameters& params) {
        if (!initialized) {
            _fixture = std::make_unique<raft_fixture_base>();
            co_await _fixture->start();
            co_await _fixture->create_simple_group(3);
            co_await _fixture->wait_for_leader(10s);
            // set election timeout to a high value to avoid elections during
            // benchmark
            _fixture->set_election_timeout(5s);
            _fixture->set_heartbeat_interval(50ms);
            initialized = true;
        }

        size_t iterations = 0;
        for (int i = 0; i < 1000; i++) {
            auto leader_id = _fixture->get_leader();
            vassert(
              leader_id.has_value(),
              "Leader must be present during the benchmark execution");
            auto& leader_node = _fixture->node(*leader_id);
            auto batches = _fixture->make_batches(
              params.batch_count, 1, params.batch_size);

            perf_tests::start_measuring_time();

            auto stages = leader_node.raft()->replicate_in_stages(
              std::move(batches), replicate_options(params.c_lvl));
            co_await std::move(stages.request_enqueued);
            if (params.stage == stage::enqueue) {
                perf_tests::stop_measuring_time();
            }
            auto result = co_await std::move(stages.replicate_finished);
            if (params.stage == stage::replicate) {
                perf_tests::stop_measuring_time();
            }
            iterations++;
            vassert(
              result.has_value(), "replication failed: {}", result.error());
        }
        co_return iterations;
    }

    ~raft_benchmark_fixture() {
        if (initialized) {
            _fixture->stop().get();
        }
    }

    bool initialized = false;
    std::unique_ptr<raft_fixture_base> _fixture;
};

PERF_TEST_CN(raft_benchmark_fixture, quorum_ack_enqueue_16B) {
    raft_bench_parameters params{
      .stage = stage::enqueue,
      .c_lvl = raft::consistency_level::quorum_ack,
      .batch_size = 16,
      .batch_count = 1,
    };
    co_return co_await run_test(params);
}

PERF_TEST_CN(raft_benchmark_fixture, quorum_ack_enqueue_16KiB) {
    raft_bench_parameters params{
      .stage = stage::enqueue,
      .c_lvl = raft::consistency_level::quorum_ack,
      .batch_size = 16_KiB,
      .batch_count = 1,
    };
    co_return co_await run_test(params);
}

PERF_TEST_CN(raft_benchmark_fixture, quorum_ack_replicate_16B) {
    raft_bench_parameters params{
      .stage = stage::replicate,
      .c_lvl = raft::consistency_level::quorum_ack,
      .batch_size = 16,
      .batch_count = 1,
    };
    co_return co_await run_test(params);
}

PERF_TEST_CN(raft_benchmark_fixture, quorum_ack_replicate_16KiB) {
    raft_bench_parameters params{
      .stage = stage::replicate,
      .c_lvl = raft::consistency_level::quorum_ack,
      .batch_size = 16_KiB,
      .batch_count = 1,
    };
    co_return co_await run_test(params);
}

PERF_TEST_CN(raft_benchmark_fixture, leader_ack_replicate_16B) {
    raft_bench_parameters params{
      .stage = stage::replicate,
      .c_lvl = raft::consistency_level::leader_ack,
      .batch_size = 16,
      .batch_count = 1,
    };
    co_return co_await run_test(params);
}

PERF_TEST_CN(raft_benchmark_fixture, leader_ack_replicate_16KiB) {
    raft_bench_parameters params{
      .stage = stage::replicate,
      .c_lvl = raft::consistency_level::leader_ack,
      .batch_size = 16_KiB,
      .batch_count = 1,
    };
    co_return co_await run_test(params);
}

} // namespace raft
