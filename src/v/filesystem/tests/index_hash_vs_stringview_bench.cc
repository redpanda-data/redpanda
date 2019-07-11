#include "hashing/xx.h"

#include <smf/random.h>

#include <benchmark/benchmark.h>

#include <set>

struct foo {
    sstring k;
    uint64_t hash;
};
bool operator==(const foo& f, uint64_t x) {
    return f.hash == x;
}
struct foo_comparator_hash {
    bool operator()(const foo& f, const uint64_t& hash) const {
        return f.hash < hash;
    };
    bool operator()(const uint64_t& hash, const foo& f) const {
        return hash < f.hash;
    };

    bool operator()(const foo& lhs, const foo& rhs) const {
        return lhs.hash < rhs.hash;
    }
};
struct foo_comparator_string {
    bool operator()(const foo& lhs, const foo& rhs) const {
        return lhs.k < rhs.k;
    }
};

static void BM_xxhash(benchmark::State& state) {
    using tset = std::set<foo, foo_comparator_hash>;
    smf::random r;
    tset s;
    for (auto i = 0; i < state.range(1); ++i) {
        foo f;
        f.k = r.next_str(state.range(0));
        f.hash = xxhash_64(f.k.data(), f.k.size());
        s.insert(std::move(f));
    }
    auto it = s.begin();
    std::advance(it, r.next() % s.size());

    for (auto _ : state) {
        s.find(*it);
    }
}
BENCHMARK(BM_xxhash)
  ->Args({256, 1 << 2}) // favor hash
  ->Args({256, 1 << 4})
  ->Args({256, 1 << 8})
  ->Args({256, 1 << 10})
  ->Args({256, 1 << 12})
  ->Args({256, 1 << 16})
  ->Args({20, 1 << 2}) // favor string
  ->Args({20, 1 << 4})
  ->Args({20, 1 << 8})
  ->Args({20, 1 << 10})
  ->Args({20, 1 << 12})
  ->Args({20, 1 << 16});

static void BM_string(benchmark::State& state) {
    using tset = std::set<foo, foo_comparator_string>;
    smf::random r;
    tset s;
    for (auto i = 0; i < state.range(1); ++i) {
        foo f;
        f.k = r.next_str(state.range(0));
        f.hash = xxhash_64(f.k.data(), f.k.size());
        s.insert(std::move(f));
    }
    auto it = s.begin();
    std::advance(it, r.next() % s.size());

    for (auto _ : state) {
        s.find(*it);
    }
}
BENCHMARK(BM_string)
  ->Args({256, 1 << 2}) // favor hash
  ->Args({256, 1 << 4})
  ->Args({256, 1 << 8})
  ->Args({256, 1 << 10})
  ->Args({256, 1 << 12})
  ->Args({256, 1 << 16})
  ->Args({20, 1 << 2}) // favor string
  ->Args({20, 1 << 4})
  ->Args({20, 1 << 8})
  ->Args({20, 1 << 10})
  ->Args({20, 1 << 12})
  ->Args({20, 1 << 16});

int main(int argc, char** argv) {
    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();
    return 0;
}
