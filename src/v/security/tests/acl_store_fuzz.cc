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
 * llvm-cov show acl_store_fuzz -instr-profile=default.profdata
 * -format=html ../src/v/security/acl.cc > cov.html
 *
 * Differential fuzzer for acl_store. ACL bindings are populated straight off
 * the wire as a matter of policy, so the key space is fully untrusted; this
 * applies a random stream of add / remove / reset / find / all_bindings / acls
 * operations and checks the store against a std::map mirror.
 *
 * Three oracles back the run:
 *   1. read-back: all_bindings() must equal the mirror after every mutation.
 *      The mirror is updated from remove_bindings()' own returned report, so
 *      this validates the report matches reality without re-implementing
 *      filter matching.
 *   2. dry-run: remove_bindings(dry_run=true) must report the same bindings as
 *      the real removal.
 *   3. routing: stored bindings are given unique, non-wildcard principals, so
 *      find().contains(a binding's own attributes) is true iff find() routed to
 *      its pattern. Asserting that for every binding proves _prefix_index (and
 *      the literal/wildcard lookups) stays exactly in sync with _acls -- no
 *      missing or stale entries. find().empty() is also cross-checked against a
 *      brute-force cover scan.
 */
#include "container/chunked_vector.h"
#include "security/acl.h"
#include "security/acl_store.h"
#include "test_utils/seastar_fuzz.h"

#include <seastar/core/future.hh>
#include <seastar/net/inet_address.hh>

#include <fmt/format.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>

namespace {

using namespace security;

constexpr std::array<resource_type, 4> resource_types{
  resource_type::topic,
  resource_type::group,
  resource_type::cluster,
  resource_type::transactional_id};

constexpr std::array<pattern_type, 2> pattern_types{
  pattern_type::literal, pattern_type::prefixed};

constexpr std::array<principal_type, 4> principal_types{
  principal_type::user,
  principal_type::role,
  principal_type::group,
  principal_type::ephemeral_user};

constexpr std::array<acl_operation, 11> operations{
  acl_operation::all,
  acl_operation::read,
  acl_operation::write,
  acl_operation::create,
  acl_operation::remove,
  acl_operation::alter,
  acl_operation::describe,
  acl_operation::cluster_action,
  acl_operation::describe_configs,
  acl_operation::alter_configs,
  acl_operation::idempotent_write};

constexpr std::array<acl_permission, 2> permissions{
  acl_permission::allow, acl_permission::deny};

// Tiny so prefixed pattern names share prefixes and exercise the radix index;
// '*' lets a literal/principal land on the wildcard slot.
constexpr std::string_view name_alphabet{"ab*"};

// A few fixed hosts; the wildcard host plus literals so host matching in
// remove filters has something to discriminate on.
acl_host host_at(size_t i) {
    switch (i % 3) {
    case 0:
        return acl_host::wildcard_host();
    case 1:
        return acl_host(ss::net::inet_address("127.0.0.1"));
    default:
        return acl_host(ss::net::inet_address("10.0.0.1"));
    }
}

class input {
public:
    explicit input(std::string_view d)
      : _d(d) {}

    bool eof() const { return _pos >= _d.size(); }

    uint8_t byte() {
        return _pos < _d.size() ? static_cast<uint8_t>(_d[_pos++]) : 0;
    }

    size_t choose(size_t n) { return byte() % n; }

    ss::sstring name() {
        const size_t len = byte() % 7;
        std::string s;
        s.reserve(len);
        for (size_t i = 0; i < len && !eof(); ++i) {
            s.push_back(name_alphabet[byte() % name_alphabet.size()]);
        }
        return ss::sstring{s.data(), s.size()};
    }

private:
    std::string_view _d;
    size_t _pos = 0;
};

resource_pattern gen_pattern(input& in) {
    return {
      resource_types.at(in.choose(resource_types.size())),
      in.name(),
      pattern_types.at(in.choose(pattern_types.size()))};
}

acl_principal gen_principal(input& in) {
    return {principal_types.at(in.choose(principal_types.size())), in.name()};
}

acl_entry gen_entry(input& in) {
    return {
      gen_principal(in),
      host_at(in.choose(3)),
      operations.at(in.choose(operations.size())),
      permissions.at(in.choose(permissions.size()))};
}

// A stable identity for a binding, used to mirror the store's contents.
std::string key(const acl_binding& b) { return fmt::format("{}", b); }

class model {
public:
    void add(input& in) {
        chunked_vector<acl_binding> bindings;
        const size_t n = in.byte() % 4 + 1;
        for (size_t i = 0; i < n; ++i) {
            bindings.push_back(next_binding(in));
        }
        for (const auto& b : bindings) {
            _mirror.insert_or_assign(key(b), b);
        }
        _store.add_bindings(bindings);
        check_readback();
    }

    void remove(input& in) {
        chunked_vector<acl_binding_filter> filters;
        const size_t n = in.byte() % 3 + 1;
        for (size_t i = 0; i < n; ++i) {
            filters.push_back(gen_filter(in));
        }

        // dry-run must report exactly what the real removal removes.
        auto dry = flatten(_store.remove_bindings(filters, true));
        auto real = flatten(_store.remove_bindings(filters, false));
        if (dry != real) {
            throw std::runtime_error("remove dry-run != real removal set");
        }
        for (const auto& k : real) {
            _mirror.erase(k);
        }
        check_readback();
    }

    void reset(input& in) {
        chunked_vector<acl_binding> bindings;
        const size_t n = in.byte() % 5;
        for (size_t i = 0; i < n; ++i) {
            bindings.push_back(next_binding(in));
        }
        _mirror.clear();
        for (const auto& b : bindings) {
            _mirror.insert_or_assign(key(b), b);
        }
        _store.reset_bindings(bindings).get();
        check_readback();
    }

    // Proves _prefix_index (plus the literal/wildcard lookups) stays in sync
    // with _acls. Stored bindings have unique, non-wildcard principals, so
    // find().contains(b's exact attributes) is true iff find() routed to b's
    // pattern -- which must hold exactly when that pattern covers the queried
    // name. Checking every stored binding therefore detects both a missing
    // index entry (covering pattern not routed) and a stale one (non-covering
    // pattern routed). The query name is biased to extend an existing pattern
    // so prefix/wildcard/literal routing is exercised on hits.
    void probe(input& in) {
        resource_type rt{};
        ss::sstring name;
        if (!_mirror.empty() && (in.byte() & 1U) != 0U) {
            const auto& seed = nth_binding(in.choose(_mirror.size()));
            rt = seed.pattern().resource();
            name = covering_name(seed.pattern(), in);
        } else {
            rt = resource_types.at(in.choose(resource_types.size()));
            name = in.name();
        }

        const auto matches = _store.find(rt, name);
        if (matches.empty() == covered(rt, name)) {
            throw std::runtime_error("find().empty() disagrees with oracle");
        }
        for (const auto& [_, b] : _mirror) {
            if (b.pattern().resource() != rt) {
                continue;
            }
            const auto& e = b.entry();
            const bool found = matches.contains(
              e.operation(), e.principal(), e.host(), e.permission());
            if (found != covers(b.pattern(), name)) {
                throw std::runtime_error(
                  "find() routing disagrees with _acls -- prefix index out of "
                  "sync");
            }
        }

        // Exercise the rest of acl_entry_set::find's predicate (operation/host
        // mismatch, wildcard-principal, role short-circuit) with a random
        // query. No oracle here -- decision semantics are authorizer_test's
        // domain; this is purely for branch coverage.
        auto principal = gen_principal(in);
        matches.contains(
          operations.at(in.choose(operations.size())),
          principal,
          host_at(in.choose(3)),
          permissions.at(in.choose(permissions.size())));
    }

    // acls(filter) enumerates the stored bindings matching a filter; every one
    // it returns must currently be in the store (i.e. in the mirror).
    void list(input& in) {
        for (const auto& b : _store.acls(gen_filter(in))) {
            if (!_mirror.contains(key(b))) {
                throw std::runtime_error("acls() returned an unknown binding");
            }
        }
    }

private:
    // A binding with a unique principal so it can be identified unambiguously
    // by find().contains() in probe(). The principal *type* stays varied (so
    // role/group paths, incl. acl_entry_set::remove_if_role and the role-cache
    // short-circuit, are exercised) but the name is unique and never the
    // wildcard "*" -- a wildcard principal would match foreign queries and
    // break the identification. Pattern and the other entry fields stay varied
    // too.
    acl_binding next_binding(input& in) {
        auto pattern = gen_pattern(in);
        const auto name = fmt::format("p{}", _ids++);
        acl_entry entry{
          acl_principal{
            principal_types.at(in.choose(principal_types.size())),
            ss::sstring{name.data(), name.size()}},
          host_at(in.choose(3)),
          operations.at(in.choose(operations.size())),
          permissions.at(in.choose(permissions.size()))};
        return {std::move(pattern), std::move(entry)};
    }

    const acl_binding& nth_binding(size_t i) const {
        return std::next(_mirror.begin(), static_cast<long>(i))->second;
    }

    // True iff find() routes name to pattern p: the wildcard ("*") or an exact
    // literal, or a prefixed pattern that is a prefix of name.
    static bool covers(const resource_pattern& p, const ss::sstring& name) {
        if (p.pattern() == pattern_type::literal) {
            return p.name() == name || p.name() == resource_pattern::wildcard;
        }
        return std::string_view{name}.starts_with(std::string_view{p.name()});
    }

    // A name that find() routes to pattern p: any name for the wildcard literal
    // "*", the name itself for an exact literal, or p's name plus a suffix for
    // a prefixed pattern (so p is a prefix of the result).
    ss::sstring covering_name(const resource_pattern& p, input& in) {
        if (p.pattern() == pattern_type::literal) {
            return p.name() == resource_pattern::wildcard ? in.name()
                                                          : p.name();
        }
        std::string n{p.name().data(), p.name().size()};
        for (size_t extra = in.byte() % 4; extra > 0 && !in.eof(); --extra) {
            n.push_back(name_alphabet[in.byte() % name_alphabet.size()]);
        }
        return ss::sstring{n.data(), n.size()};
    }

    // True iff some stored pattern of type rt covers name (see covers()).
    bool covered(resource_type rt, const ss::sstring& name) const {
        for (const auto& [_, b] : _mirror) {
            if (b.pattern().resource() == rt && covers(b.pattern(), name)) {
                return true;
            }
        }
        return false;
    }

    void check_readback() {
        std::set<std::string> got;
        auto bindings = _store.all_bindings().get();
        for (const auto& b : bindings) {
            got.insert(key(b));
        }
        std::set<std::string> want;
        for (const auto& [k, _] : _mirror) {
            want.insert(k);
        }
        if (got != want) {
            throw std::runtime_error("all_bindings() != mirror");
        }
    }

    acl_binding_filter gen_filter(input& in) {
        switch (in.choose(4)) {
        case 0:
            // Match everything -> mass prune (heaviest stress on the index
            // sync invariant).
            return {resource_pattern_filter{}, acl_entry_filter{}};
        case 1:
            // Exact filter for an existing binding, if any.
            if (!_mirror.empty()) {
                const auto& b = nth_binding(in.choose(_mirror.size()));
                return {
                  resource_pattern_filter{b.pattern()},
                  acl_entry_filter{b.entry()}};
            }
            [[fallthrough]];
        case 2:
            // All entries of a (possibly non-existent) pattern.
            return {
              resource_pattern_filter{gen_pattern(in)}, acl_entry_filter{}};
        default:
            // A generated pattern + a single generated entry.
            return {
              resource_pattern_filter{gen_pattern(in)},
              acl_entry_filter{gen_entry(in)}};
        }
    }

    static std::multiset<std::string>
    flatten(const chunked_vector<chunked_vector<acl_binding>>& res) {
        std::multiset<std::string> out;
        for (const auto& per_filter : res) {
            for (const auto& b : per_filter) {
                out.insert(key(b));
            }
        }
        return out;
    }

    acl_store _store;
    std::map<std::string, acl_binding> _mirror;
    // source of unique principal names for stored bindings
    size_t _ids = 0;
};

constexpr uint8_t num_ops = 5;

void run(std::string_view data) {
    input in(data);
    model m;
    while (!in.eof()) {
        switch (in.byte() % num_ops) {
        case 0:
            m.add(in);
            break;
        case 1:
            m.remove(in);
            break;
        case 2:
            m.reset(in);
            break;
        case 3:
            m.probe(in);
            break;
        case 4:
            m.list(in);
            break;
        default:
            break;
        }
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    std::string input(reinterpret_cast<const char*>(data), size);
    seastar_fuzz::test_one_input([input = std::move(input)] { run(input); });
    return 0;
}
