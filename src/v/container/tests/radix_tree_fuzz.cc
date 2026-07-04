/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 *
 * Coverage
 * ========
 *
 * llvm-profdata merge -sparse default.profraw -o default.profdata
 *
 * llvm-cov show radix_tree_fuzz -instr-profile=default.profdata
 * -format=html ../src/v/container/radix_tree.h > cov.html
 *
 * Differential fuzzer: a random stream of operations is applied to the
 * radix_tree under test and to a std::map oracle in lockstep. The oracle's
 * answers are correct by construction (linear scans), so any divergence in
 * membership, stored value, or the set/order of prefix matches is a bug.
 *
 * Each key carries a fuzzer-chosen alphabet so the engine itself allocates
 * effort across the spectrum rather than a hardcoded split. The symbol pool is
 * deliberately adversarial (NUL, a high-bit / signed-char byte, a separator, a
 * digit, letters) so edge-case bytes land at branch points, and a per-key
 * cardinality selects the tree shape: a tiny alphabet maximizes shared
 * prefixes (edge splitting / node folding / deep chains), a medium one drives
 * wide fan-out nodes, and raw bytes cover the full 0-255 range for breadth.
 */
#include "container/radix_tree.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <map>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

constexpr size_t max_key_len = 32;

// Adversarial symbol pool drawn on by the small/medium alphabets: NUL and a
// high-bit (signed-char) byte stress terminator / sign handling, a separator
// and digit add realistic ACL-name bytes, and the letters fill it out. Keys
// share prefixes over these, so edge-case bytes occur at branch points rather
// than only in flat leaves.
constexpr std::array<char, 16> symbol_pool{
  '\0',
  '\xff',
  '/',
  '-',
  '7',
  '0',
  'a',
  'b',
  'c',
  'd',
  'e',
  'z',
  'A',
  'B',
  'Z',
  '.'};

// Per-key alphabet cardinality. Tiny alphabets maximize shared prefixes
// (splits/folds/deep chains); the medium one drives wide fan-out nodes; 0 is a
// sentinel for "raw bytes", covering the full 0-255 range for breadth.
constexpr std::array<size_t, 4> cardinalities{2, 4, 16, 0};

// Cursor over the fuzzer input. Reads run dry gracefully: once the bytes are
// exhausted every read yields 0 / an empty key, so a truncated program still
// terminates rather than reading out of bounds.
class input {
public:
    explicit input(std::string_view d)
      : _d(d) {}

    bool eof() const { return _pos >= _d.size(); }

    uint8_t byte() {
        return _pos < _d.size() ? static_cast<uint8_t>(_d[_pos++]) : 0;
    }

    // A key is a 1-byte alphabet selector, a 1-byte length, then that many
    // bytes. The selector picks a cardinality: each key byte is folded into
    // that many symbols of the pool to force shared prefixes, or passed through
    // verbatim (cardinality 0) to cover arbitrary byte keys.
    std::string key() {
        const size_t card = cardinalities.at(byte() % cardinalities.size());
        const size_t len = byte() % (max_key_len + 1);
        std::string k;
        k.reserve(len);
        for (size_t i = 0; i < len && !eof(); ++i) {
            const uint8_t b = byte();
            k.push_back(
              card == 0 ? static_cast<char>(b) : symbol_pool.at(b % card));
        }
        return k;
    }

private:
    std::string_view _d;
    size_t _pos = 0;
};

// The radix_tree under test alongside a std::map oracle. Every mutation is
// mirrored to both and immediately reconciled; queries are checked against the
// oracle's brute-force answer.
class model {
public:
    void insert(const std::string& k, int v) {
        _tree.insert(k, v);
        _ref[k] = v;
        check_consistency();
    }

    void erase(const std::string& k) {
        const bool in_tree = _tree.erase(k);
        const bool in_ref = _ref.erase(k) > 0;
        if (in_tree != in_ref) {
            throw std::runtime_error("erase return mismatch");
        }
        check_consistency();
    }

    void find(const std::string& k) const {
        const int* got = _tree.find(k);
        const auto it = _ref.find(k);
        const bool want = it != _ref.end();
        if ((got != nullptr) != want) {
            throw std::runtime_error("find membership mismatch");
        }
        if (got != nullptr && *got != it->second) {
            throw std::runtime_error("find value mismatch");
        }
    }

    void contains(const std::string& k) const {
        if (_tree.contains(k) != _ref.contains(k)) {
            throw std::runtime_error("contains mismatch");
        }
    }

    // Every stored key that is a prefix of `q` must be reported exactly once,
    // shortest-prefix-first. Any two stored prefixes of the same query differ
    // in length (one is a prefix of the other), so ordering the oracle by
    // length is a strict total order matching the tree's documented
    // shortest-first traversal.
    void prefix(const std::string& q) const {
        std::vector<std::pair<std::string, int>> got;
        _tree.for_each_prefix_of(q, [&](std::string_view k, const int& v) {
            got.emplace_back(std::string{k}, v);
        });

        std::vector<std::pair<std::string, int>> want;
        for (const auto& [k, v] : _ref) {
            if (
              std::string_view{q}.substr(0, std::min(k.size(), q.size()))
              == k) {
                want.emplace_back(k, v);
            }
        }
        std::ranges::sort(
          want, {}, [](const auto& e) { return e.first.size(); });

        if (got != want) {
            throw std::runtime_error("prefix match set/order mismatch");
        }
    }

    void clear() {
        _tree.clear();
        _ref.clear();
        check_consistency();
    }

private:
    void check_consistency() const {
        if (_tree.size() != _ref.size()) {
            throw std::runtime_error("size mismatch");
        }
        for (const auto& [k, v] : _ref) {
            const int* got = _tree.find(k);
            if (got == nullptr || *got != v) {
                throw std::runtime_error("stored key missing or mismatched");
            }
        }
    }

    radix_tree<int> _tree;
    std::map<std::string, int> _ref;
};

enum class op : uint8_t {
    insert,
    erase,
    find,
    contains,
    prefix,
    clear,
};
constexpr uint8_t num_ops = 6;

void run(std::string_view data) {
    input in(data);
    model m;
    while (!in.eof()) {
        switch (static_cast<op>(in.byte() % num_ops)) {
        case op::insert: {
            const auto k = in.key();
            m.insert(k, static_cast<int>(in.byte()));
            break;
        }
        case op::erase:
            m.erase(in.key());
            break;
        case op::find:
            m.find(in.key());
            break;
        case op::contains:
            m.contains(in.key());
            break;
        case op::prefix:
            m.prefix(in.key());
            break;
        case op::clear:
            m.clear();
            break;
        }
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    run(std::string_view(reinterpret_cast<const char*>(data), size));
    return 0;
}
