
#include <cstring>
#include <memory>

#include <benchmark/benchmark.h>

#include "hashing/jump_consistent_hash.h"
#include "hashing/xx.h"

using namespace v;  // NOLINT

static uint64_t
fixed_hash(const std::string &topic, uint32_t partition) {
  const std::size_t kBufSize = 50;
  char buf[kBufSize];
  std::memset(buf, 0, kBufSize);
  std::memcpy(buf, (char *)&partition, 4);
  std::memcpy(buf + 4, topic.c_str(), std::min(kBufSize - 4, topic.size()));
  return xxhash_64(buf, kBufSize);
}

static uint64_t
stack_hash(const std::string &topic, uint32_t partition) {
  const std::size_t kBufSize = topic.size() + 4;
  char buf[kBufSize];
  std::memset(buf, 0, kBufSize);
  std::memcpy(buf, (char *)&partition, 4);
  std::memcpy(buf + 4, topic.c_str(), topic.size());
  return xxhash_64(buf, kBufSize);
}

static uint64_t
incremental_hash(const std::string &topic, uint32_t partition) {
  incremental_xxhash64 inc;
  inc.update((char *)&partition, sizeof(partition));
  inc.update(topic.data(), topic.size());
  return inc.digest();
}

static void
BM_fixed_hash(benchmark::State &state) {
  for (auto _ : state) {
    state.PauseTiming();
    std::string x;
    x.reserve(state.range(0));
    for (auto i = 0; i < state.range(0); ++i) {
      x.push_back('x');
    }
    state.ResumeTiming();
    benchmark::DoNotOptimize(fixed_hash(x, state.range(0)));
  }
}
BENCHMARK(BM_fixed_hash)
  ->Args({1 << 1, 1 << 1})
  ->Args({1 << 4, 1 << 4})
  ->Args({1 << 8, 1 << 8});
//->Args({1 << 16, 1 << 16});

static void
BM_stackhash(benchmark::State &state) {
  for (auto _ : state) {
    state.PauseTiming();
    std::string x;
    x.reserve(state.range(0));
    for (auto i = 0; i < state.range(0); ++i) {
      x.push_back('x');
    }
    state.ResumeTiming();
    benchmark::DoNotOptimize(stack_hash(x, state.range(0)));
  }
}
BENCHMARK(BM_stackhash)
  ->Args({1 << 1, 1 << 1})
  ->Args({1 << 4, 1 << 4})
  ->Args({1 << 8, 1 << 8});
//->Args({1 << 16, 1 << 16});

static void
BM_incremental_hash(benchmark::State &state) {
  for (auto _ : state) {
    state.PauseTiming();
    std::string x;
    x.reserve(state.range(0));
    for (auto i = 0; i < state.range(0); ++i) {
      x.push_back('x');
    }
    state.ResumeTiming();
    benchmark::DoNotOptimize(incremental_hash(x, state.range(0)));
  }
}
BENCHMARK(BM_incremental_hash)
  ->Args({1 << 1, 1 << 1})
  ->Args({1 << 4, 1 << 4})
  ->Args({1 << 8, 1 << 8});
//->Args({1 << 16, 1 << 16});

BENCHMARK_MAIN();
