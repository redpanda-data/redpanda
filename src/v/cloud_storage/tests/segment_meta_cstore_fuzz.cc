/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License included in
 * the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with the Business
 * Source License, use of this software will be governed by the Apache License,
 * Version 2.0
 *
 * Coverage
 * ========
 *
 * llvm-profdata merge -sparse default.profraw -o default.profdata
 *
 * llvm-cov show segment_meta_cstore_fuzz_rpfixture
 * -instr-profile=default.profdata -format=html ../src/v/bytes/iobuf.h
 * ../src/v/bytes/iobuf.cc > cov.html
 */

#include "bytes/iobuf.h"
#include "cloud_storage/segment_meta_cstore.h"
#include "cloud_storage/types.h"
#include "model/fundamental.h"
#include "reflection/to_tuple.h"
#include "serde/type_str.h"

#include <absl/container/btree_map.h>
#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <numeric>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unistd.h>
#include <utility>
#include <variant>

class cstore_ops {
    cloud_storage::segment_meta_cstore cstore{};
    absl::btree_map<model::offset, cloud_storage::segment_meta> reference{};

    cloud_storage::segment_meta_cstore::const_iterator reader_it = cstore.end();
    cloud_storage::segment_meta_cstore::const_iterator reader_end
      = cstore.end();

    // invariant: this contains a valid cstore binary image
    iobuf serialized_cstore{};

public:
    cstore_ops()
      : serialized_cstore{cstore.to_iobuf()} {}

    void self_moves() {
        auto tmp = cloud_storage::segment_meta_cstore{
          std::move(cstore)};    // move constructor
        cstore = std::move(tmp); // move assignment
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wself-move"
        cstore = std::move(cstore); // self-move assignment
#pragma clang diagnostic pop
    }

    void reset_reader(size_t index) {
        reader_end = cstore.end();
        reader_it = std::next(cstore.begin(), std::min(index, cstore.size()));
    }

    void reset_reader(model::offset off) {
        reader_end = cstore.end();
        reader_it = cstore.lower_bound(off);
    }

    void consume_reader(size_t quantity) {
        for (; quantity != 0 && reader_it != reader_end;
             ++reader_it, --quantity) {
            auto value = *reader_it;
            (void)value;
        }
    }

    void append_segment(cloud_storage::segment_meta smeta) {
        auto opt_last = cstore.last_segment();
        if (opt_last) {
            // ensure segment will be last
            auto delta = smeta.committed_offset - smeta.base_offset;
            smeta.base_offset = opt_last->committed_offset + model::offset{1};
            smeta.committed_offset = smeta.base_offset + delta;
        }

        if (reference.contains(smeta.base_offset)) {
            throw std::runtime_error{
              fmt::format("base_offset {} already exists", smeta.base_offset)};
        }

        cstore.insert(smeta);
        reference[smeta.base_offset] = smeta;
    }

    // clean version is for smeta that is correctly aligned with pre-existing
    // base and committed offsets
    void replace_segment_clean(
      cloud_storage::segment_meta smeta,
      uint16_t index,
      uint16_t num_of_replacements) {
        if (index >= reference.size()) {
            // just append
            append_segment(smeta);
            return;
        }
        // side step a degenerate cases
        num_of_replacements = std::max(num_of_replacements, uint16_t(1));
        auto end_index = size_t(index + num_of_replacements);
        end_index = std::min(end_index, reference.size());

        if (index == end_index) {
            // this can happen in a degenerate case
            return;
        }

        // extract range to be replaced
        auto repl_beg = std::next(reference.begin(), index);
        auto repl_end = std::next(reference.begin(), end_index);

        // make smeta a replacement for range
        smeta.base_offset = repl_beg->second.base_offset;
        smeta.committed_offset = std::prev(repl_end)->second.committed_offset;

        // replace
        cstore.insert(smeta);
        reference.erase(repl_beg, repl_end);
        reference[smeta.base_offset] = smeta;
    }

    void prepend_segment(cloud_storage::segment_meta smeta) {
        if (reference.empty()) {
            append_segment(smeta);
            return;
        }

        // adapt smeta
        auto delta = smeta.committed_offset - smeta.base_offset;
        smeta.committed_offset = reference.begin()->second.base_offset
                                 - model::offset{1};
        smeta.base_offset = smeta.committed_offset - delta;

        reference[smeta.base_offset] = smeta;
        cstore.insert(smeta);
    }

    // insert a replacement that will have a base offset before the first one in
    // the store
    void replace_segment_clean_in_front(
      cloud_storage::segment_meta smeta, size_t num_of_replacements) {
        if (reference.empty() || num_of_replacements == 0) {
            prepend_segment(smeta);
            return;
        }
        // num_of_replacements is at least 1
        num_of_replacements = std::min(num_of_replacements, reference.size());
        auto repl_end = std::next(reference.begin(), num_of_replacements);

        // adapt smeta
        auto delta = smeta.committed_offset - smeta.base_offset;
        smeta.committed_offset = reference.begin()->second.base_offset
                                 - model::offset{1};
        smeta.base_offset = smeta.committed_offset - delta;
        smeta.committed_offset = std::prev(repl_end)->second.committed_offset;

        // replace them
        cstore.insert(smeta);
        reference.erase(reference.begin(), repl_end);
        reference[smeta.base_offset] = smeta;
    }

    void clear() {
        cstore = cloud_storage::segment_meta_cstore{};
        reference.clear();
    }

    void truncate(model::offset new_start_offset) {
        auto new_start = std::find_if(
          reference.begin(), reference.end(), [new_start_offset](auto& kv) {
              return kv.second.base_offset >= new_start_offset;
          });
        reference.erase(reference.begin(), new_start);
        cstore.prefix_truncate(new_start_offset);
    }

    void self_serialize() {
        auto buf = cstore.to_iobuf();
        auto tmp = cloud_storage::segment_meta_cstore{};
        tmp.from_iobuf(std::move(buf));

        if (cstore != tmp) {
            throw std::runtime_error{
              fmt::format("cstore != tmp after serialization")};
        }
    }

    void serialize() { serialized_cstore = cstore.to_iobuf(); }

    auto deserialize() {
        cstore.from_iobuf(
          serialized_cstore.share(0, serialized_cstore.size_bytes()));

        reference.clear();
        for (auto seg : cstore) {
            reference[seg.base_offset] = seg;
        }
    }

public:
    /*
     * Check consistency of cstore with reference.
     */
    void check() {
        if (reference.size() != cstore.size()) {
            throw std::runtime_error{fmt::format(
              "reference size {} != cstore size {}",
              reference.size(),
              cstore.size())};
            ;
        }

        // if you squint, you'll see std::mismatch (but cstore_it is not
        // copyable)
        auto ref_it = reference.begin();
        auto cstore_it = cstore.begin();
        for (; ref_it != reference.end(); ++ref_it, ++cstore_it) {
            if (ref_it->second != *cstore_it) {
                throw std::runtime_error{fmt::format(
                  "reference {} != cstore {} @ index {}",
                  ref_it->second,
                  *cstore_it,
                  cstore_it.index())};
            }
        }

        if (reader_it != reader_end) {
            // this is just a nudge to check for up in the materialize() path
            auto _ = *reader_it;
            (void)_;
        }

        // this verifies that serialized_cstore is valid
        auto tmp = cloud_storage::segment_meta_cstore{};
        tmp.from_iobuf(
          serialized_cstore.share(0, serialized_cstore.size_bytes()));
    }
};

class Tape {
    std::string_view program_;
    std::string_view::const_iterator pc_;

public:
    // signal that the program should terminate normally
    class end_of_program : std::exception {};

    explicit Tape(std::string_view program)
      : program_(program)
      , pc_(program.cbegin()) {}

    template<typename T>
    T read() {
        std::array<char, sizeof(T)> buf;
        if (std::distance(pc_, program_.cend()) < buf.size()) {
            throw end_of_program();
        }
        std::copy_n(pc_, buf.size(), buf.begin());
        auto ret = std::bit_cast<T>(buf);
        std::advance(pc_, sizeof(T));
        return ret;
    }

    // This produces a segment meta that do not respect invariants in
    // redpanda. the only invariant is base_offset < committed_offset
    // furthermore, base_offset is always the same, just a placeholer
    auto read_generic_segment_meta() -> cloud_storage::segment_meta {
        constexpr static auto base_offset = model::offset{10};
        return {
          .is_compacted = read<uint8_t>() != 0,
          .size_bytes = read<size_t>(),
          .base_offset = base_offset,
          .committed_offset
          = base_offset
            + std::max(model::offset{read<uint16_t>()}, model::offset{0})
            + model::offset{1},
          .base_timestamp = read<model::timestamp>(),
          .max_timestamp = read<model::timestamp>(),
          .delta_offset = read<model::offset_delta>(),
          .ntp_revision = read<model::initial_revision_id>(),
          .archiver_term = read<model::term_id>(),
          .segment_term = read<model::term_id>(),
          .delta_offset_end = read<model::offset_delta>(),
          .sname_format = read<cloud_storage::segment_name_format>(),
          .metadata_size_hint = read<uint64_t>(),
        };
    }

    auto read_simplified_segment_meta() -> cloud_storage::segment_meta {
        constexpr static auto base_offset = model::offset{10};
        return {
          .is_compacted = false,
          .size_bytes = 1024u,
          .base_offset = base_offset,
          .committed_offset
          = base_offset
            + std::max(model::offset{read<uint16_t>()}, model::offset{0})
            + model::offset{1},
          .base_timestamp = {},
          .max_timestamp = {},
          .delta_offset = {},
          .ntp_revision = {},
          .archiver_term = {},
          .segment_term = {},
          .delta_offset_end = {},
          .sname_format = {},
          .metadata_size_hint = {},
        };
    }
};

struct noop {
    auto operator()(cstore_ops& ops) const {}
};
struct self_move_op {
    auto operator()(cstore_ops& ops) const { ops.self_moves(); }
};

struct append_segment_op {
    cloud_storage::segment_meta smeta{};
    void setup(Tape& tape) { smeta = tape.read_generic_segment_meta(); }
    auto operator()(cstore_ops& ops) const { ops.append_segment(smeta); }
};

struct replace_segment_clean_op {
    cloud_storage::segment_meta smeta{};
    uint16_t index{};
    uint16_t num_of_replacements{};
    void setup(Tape& tape) {
        smeta = tape.read_generic_segment_meta();
        index = tape.read<uint16_t>();
        num_of_replacements = tape.read<uint16_t>();
    }
    auto operator()(cstore_ops& ops) const {
        ops.replace_segment_clean(smeta, index, num_of_replacements);
    }
};

struct prepend_segment_op {
    cloud_storage::segment_meta smeta{};
    void setup(Tape& tape) { smeta = tape.read_generic_segment_meta(); }
    auto operator()(cstore_ops& ops) const { ops.prepend_segment(smeta); }
};

struct replace_segment_clean_in_front_op {
    cloud_storage::segment_meta smeta{};
    size_t num_of_replacements{};
    void setup(Tape& tape) {
        smeta = tape.read_generic_segment_meta();
        num_of_replacements = tape.read<size_t>();
    }
    auto operator()(cstore_ops& ops) const {
        ops.replace_segment_clean_in_front(smeta, num_of_replacements);
    }
};

struct clear_op {
    auto operator()(cstore_ops& ops) const { ops.clear(); }
};

struct truncate_op {
    model::offset new_start_offset{};
    void setup(Tape& tape) { new_start_offset = tape.read<model::offset>(); }
    auto operator()(cstore_ops& ops) const { ops.truncate(new_start_offset); }
};

struct self_serialize_op {
    auto operator()(cstore_ops& ops) const { ops.self_serialize(); }
};

struct reset_reader_op {
    model::offset start_offset{};
    void setup(Tape& tape) { start_offset = tape.read<model::offset>(); }
    auto operator()(cstore_ops& ops) const { ops.reset_reader(start_offset); }
};

struct consume_reader_op {
    size_t quantity{};
    void setup(Tape& tape) { quantity = tape.read<size_t>(); }
    auto operator()(cstore_ops& ops) const { ops.consume_reader(quantity); }
};

struct serialize_op {
    auto operator()(cstore_ops& ops) const { ops.serialize(); }
};

struct deserialize_op {
    auto operator()(cstore_ops& ops) const { ops.deserialize(); }
};

using cstore_operation = std::variant<
  noop,
  self_move_op,
  append_segment_op,
  replace_segment_clean_op,
  replace_segment_clean_in_front_op,
  prepend_segment_op,
  clear_op,
  truncate_op,
  self_serialize_op,
  reset_reader_op,
  consume_reader_op,
  serialize_op,
  deserialize_op>;

// SIN SECTION
// this is a sections of ODR sins to appease the daemons of the linker

auto cloud_storage::operator<<(std::ostream& os, const segment_meta& s)
  -> std::ostream& {
    return os << fmt::format(
             "{{is_compacted: {}, size_bytes: {}, base_offset: {}, "
             "committed_offset: "
             "{}, base_timestamp: {}, max_timestamp: {}, delta_offset: {}, "
             "ntp_revision: {}, archiver_term: {}, segment_term: {}, "
             "delta_offset_end: {}, sname_format: {}, metadata_size_hint: {}}}",
             s.is_compacted,
             s.size_bytes,
             s.base_offset,
             s.committed_offset,
             s.base_timestamp,
             s.max_timestamp,
             s.delta_offset,
             s.ntp_revision,
             s.archiver_term,
             s.segment_term,
             s.delta_offset_end,
             s.sname_format,
             s.metadata_size_hint);
}

auto cloud_storage::operator<<(std::ostream& os, const segment_name_format& sn)
  -> std::ostream& {
    return os << fmt::format("{}", unsigned(sn));
}

// END OF SIN SECTION

template<>
struct fmt::formatter<cstore_operation>
  : public fmt::formatter<std::string_view> {
    auto format(const cstore_operation& op, auto& ctx) const {
        auto f = [&] {
            return std::visit<std::string>(
              []<typename OP>(OP const& op) {
                  if constexpr (reflection::arity<OP>() == 0) {
                      return fmt::format("{}", serde::type_str<OP>());
                  } else if constexpr (reflection::arity<OP>() == 1) {
                      return fmt::format(
                        "{}({})",
                        serde::type_str<OP>(),
                        std::get<0>(reflection::to_tuple(op)));
                  } else if constexpr (reflection::arity<OP>() == 2) {
                      return fmt::format(
                        "{}({},{})",
                        serde::type_str<OP>(),
                        std::get<0>(reflection::to_tuple(op)),
                        std::get<1>(reflection::to_tuple(op)));
                  } else if constexpr (reflection::arity<OP>() == 3) {
                      return fmt::format(
                        "{}({},{},{})",
                        serde::type_str<OP>(),
                        std::get<0>(reflection::to_tuple(op)),
                        std::get<1>(reflection::to_tuple(op)),
                        std::get<2>(reflection::to_tuple(op)));
                  } else {
                      static_assert(
                        base::unsupported_type<OP>::value, "unsupported type");
                  }
              },
              op);
        };
        return formatter<std::string_view>::format(f(), ctx);
    }
};

/*
 * a program is a series of bytes. the structure of a program is:
 *
 *   [{op_code[, operands...]}*]
 *
 * where the set of operands are dependent on the op_code. the program
 * ends when the end of the program is reached. if the operands for the
 * final op_code are truncated then the program should terminate
 * normally.
 */
class driver {
    Tape tape;
    cstore_ops m_{};
    std::vector<cstore_operation> trace_{};

public:
    explicit driver(std::string_view program)
      : tape{program} {}
    void print_trace() const {
        // this could be a fmt::print + fmt::join if we had ranges
        fmt::print("TRACE:\n");
        for (auto i = 0u; i < trace_.size(); ++i) {
            fmt::print("\t{:>4} {}\n", i, trace_[i]);
        }
    }

    bool operator()() {
        // default op
        auto next_op = cstore_operation{noop{}};
        try {
            // try to construct the next op, this could fail if there are not
            // enough bytes
            auto next_op_idx = tape.read<uint8_t>()
                               % std::variant_size_v<cstore_operation>;
            [&]<size_t... Is>(std::index_sequence<Is...>) {
                ([&] {
                    if (next_op_idx == Is) {
                        next_op = cstore_operation{std::in_place_index<Is>};
                        if constexpr (requires(
                                        std::variant_alternative_t<
                                          Is,
                                          cstore_operation> op,
                                        Tape t) { op.setup(t); }) {
                            // call setup if op has it
                            std::get<Is>(next_op).setup(tape);
                        }

                        return true;
                    }
                    return false;
                }()
                 || ...);
            }(std::make_index_sequence<
              std::variant_size_v<cstore_operation>>{});
        } catch (const Tape::end_of_program&) {
            return false;
        }
        trace_.push_back(next_op);
        std::visit([&](auto& op) { op(m_); }, next_op);
        return true;
    }

    void check() { m_.check(); }
};

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    // NOLINTNEXTLINE
    std::string_view d(reinterpret_cast<const char*>(data), size);
    auto p = driver{d};

    try {
        while (p()) {
            p.check();
        }
    } catch (...) {
        p.print_trace();
        throw;
    }

    return 0;
}
