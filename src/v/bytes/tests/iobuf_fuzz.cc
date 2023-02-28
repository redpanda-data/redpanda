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
 * llvm-cov show src/v/bytes/tests/fuzz_iobuf -instr-profile=default.profdata
 * -format=html ../src/v/bytes/iobuf.h ../src/v/bytes/iobuf.cc > cov.html
 */
#include "bytes/bytes.h"
#include "bytes/iobuf.h"
#include "bytes/scattered_message.h"
#include "vassert.h"

#include <numeric>

/*
 * Holds an iobuf and a reference container. When an operation (e.g. append)
 * is applied it is performed equally to the iobuf and the reference. At any
 * time consistency between the iobuf and the reference can be verified with
 * check().
 */
class iobuf_ops {
    iobuf buf;
    std::deque<std::string_view> ref;

public:
    void moves() {
        auto tmp(std::move(buf)); // move constructor
        buf = std::move(tmp);     // move assignment
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wself-move"
        buf = std::move(buf); // self-move assignment
#pragma clang diagnostic pop
    }

    void reserve_memory(size_t size) {
        buf.reserve_memory(size);
        // no content change to reference container
    }

    void append_char_array(std::string_view payload) {
        buf.append(payload.data(), payload.size());
        ref.push_back(payload);
    }

    void append_uint8_array(std::string_view payload) {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
        auto uint8y_payload = reinterpret_cast<const uint8_t*>(payload.data());
        buf.append(uint8y_payload, payload.size());
        ref.push_back(payload);
    }

    void append_temporary_buffer(std::string_view payload) {
        buf.append(ss::temporary_buffer<char>(payload.data(), payload.size()));
        ref.push_back(payload);
    }

    void append_iobuf(std::string_view payload) {
        iobuf tmp;
        tmp.append(payload.data(), payload.size());
        buf.append(std::move(tmp));
        ref.push_back(payload);
    }

    void append_fragments(std::string_view payload) {
        /*
         * footgun alert: if you append from an iobuf share then the backing
         * memory is also shared. so if you do something like this:
         *
         *    buf.append_fragments(buf.share(0, size));
         *
         * then it can end up that if you append to `buf` that you also
         * overwrite data at the front of the buffer. this same scenario can
         * probably be created when combining ops with iobuf::share.
         */
        iobuf tmp;
        tmp.append(payload.data(), payload.size());
        buf.append_fragments(std::move(tmp));
        ref.push_back(payload);
    }

    void prepend_temporary_buffer(std::string_view payload) {
        buf.prepend(ss::temporary_buffer<char>(payload.data(), payload.size()));
        ref.push_front(payload);
    }

    void prepend_iobuf(std::string_view payload) {
        iobuf tmp;
        tmp.append(payload.data(), payload.size());
        buf.prepend(std::move(tmp));
        ref.push_front(payload);
    }

    void trim_front(size_t size) {
        buf.trim_front(size);
        while (size && !ref.empty()) {
            if (size >= ref.front().size()) {
                size -= ref.front().size();
                ref.pop_front();
            } else {
                ref.front().remove_prefix(size);
                return;
            }
        }
    }

    void trim_back(size_t size) {
        buf.trim_back(size);
        while (size && !ref.empty()) {
            if (size >= ref.back().size()) {
                size -= ref.back().size();
                ref.pop_back();
            } else {
                ref.back().remove_suffix(size);
                size = 0;
                break;
            }
        }
    }

    void clear() {
        buf.clear();
        ref.clear();
    }

    void hexdump(size_t size) { auto s = buf.hexdump(size); }

    void print() {
        std::stringstream ss;
        ss << buf;
    }

    void iobuf_as_scattered() {
        auto s = ::iobuf_as_scattered(buf.share(0, buf.size_bytes()));
        auto p = std::move(s).release();
        iobuf tmp;
        for (auto& t : p.release()) {
            tmp.append(std::move(t));
        }
        if (tmp != buf) {
            throw std::runtime_error(
              "Iobuf as scattered message doesn't match original data");
        }
    }

    /*
     * covers
     *   iobuf::copy
     *   iobuf_copy (free function)
     *   iobuf::operator==(iobuf)
     *   iobuf::operator!=(iobuf)
     */
    void copy() const {
        auto iob0 = buf.copy();
        if (!(iob0 == buf)) {
            throw std::runtime_error(fmt::format(
              "Iobuf (sz={}) copy (sz={}) not identical (iobuf::copy)",
              buf.size_bytes(),
              iob0.size_bytes()));
        }

        auto it = iobuf::iterator_consumer(buf.cbegin(), buf.cend());
        auto iob1 = iobuf_copy(it, buf.size_bytes());
        if (iob1 != buf) {
            throw std::runtime_error(fmt::format(
              "Iobuf (sz={}) copy (sz={}) not identical (iobuf_copy)",
              buf.size_bytes(),
              iob1.size_bytes()));
        }

        iob0.append("a", 1);
        if (iob0 == iob1) {
            throw std::runtime_error(
              "Iobufs compare equal with differing sizes");
        }

        iob1.append("b", 1);
        if (iob0 == iob1) {
            throw std::runtime_error(
              "Iobufs compare equal with differing final byte");
        }
    }

public:
    /*
     * Check consistency of iobuf with reference.
     */
    void check() const {
        const auto ref_size = std::accumulate(
          ref.begin(),
          ref.end(),
          size_t(0),
          [](size_t acc, std::string_view sv) { return acc + sv.size(); });

        if (buf.empty() != (ref_size == 0)) {
            throw std::runtime_error(fmt::format(
              "Iobuf empty state {} != reference empty state {}",
              buf.empty(),
              ref_size == 0));
        }

        if (buf.size_bytes() != ref_size) {
            throw std::runtime_error(fmt::format(
              "Iobuf size {} != reference size {}",
              buf.size_bytes(),
              ref_size));
        }

        check_equals();
    }

private:
    /*
     * Compare contents of iobuf and reference container.
     */
    void check_equals() const {
        struct ref_ctx {
            std::deque<std::string_view>::const_iterator it, end;
            std::string_view sv;

            explicit ref_ctx(const std::deque<std::string_view>& ref)
              : it(ref.cbegin())
              , end(ref.cend()) {
                if (it != end) {
                    sv = *it;
                }
            }

            std::string_view next(size_t limit) {
                vassert(it != end, "");
                auto ret = sv.substr(0, limit);
                vassert(ret.size() <= sv.size(), "");
                sv.remove_prefix(ret.size());
                if (sv.empty()) {
                    ++it;
                    if (it != end) {
                        sv = *it;
                    }
                }
                return ret;
            }
        };

        // for string_view operators. skip if the buffer is big.
        std::optional<std::string> s;
        // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers)
        if (buf.size_bytes() < (1ULL << 18ULL)) {
            s.emplace();
        }

        ref_ctx ctx(ref);
        auto in = iobuf::iterator_consumer(buf.cbegin(), buf.cend());
        in.consume(buf.size_bytes(), [&s, &ctx](const char* data, size_t size) {
            if (s.has_value()) {
                s.value().append(data, size);
            }
            std::string_view in(data, size);
            while (!in.empty()) {
                auto ref = ctx.next(in.size());
                if (std::memcmp(in.data(), ref.data(), ref.size()) != 0) {
                    throw std::runtime_error(fmt::format(
                      "Iobuf contents differ from reference contents"));
                }
                in.remove_prefix(ref.size());
            }
            return ss::stop_iteration::no;
        });

        if (s.has_value()) {
            // coverage for iobuf::operator==(std:string_view) (equal)
            if (!(buf == std::string_view(s.value()))) {
                throw std::runtime_error("Iobuf contents are not identical for "
                                         "string_view comparison");
            }
            // coverage for iobuf::operator==(std:string_view) (not-equal)
            if (!s.value().empty()) {
                s.value().back()++;
                if (buf == std::string_view(s.value())) {
                    throw std::runtime_error(
                      "Iobuf contents are not identical for "
                      "string_view comparison");
                }
                s.value().back()--;
            }
            // coverage for iobuf::operator!=(std:string_view) (size difference)
            s.value().append("a");
            if (!(buf != std::string_view(s.value()))) {
                throw std::runtime_error(
                  "Iobuf contents are identical for string_view comparison");
            }
        }
    }
};

enum class op_type : uint8_t {
    copy,
    moves,
    trim_front,
    trim_back,
    clear,
    print,
    iobuf_as_scattered,
    hexdump,
    reserve_memory,
    prepend_temporary_buffer,
    prepend_iobuf,
    append_iobuf,
    append_temporary_buffer,
    append_fragments,
    append_uint8_array,
    append_char_array,
    max = append_char_array,
};

template<>
struct fmt::formatter<op_type> : formatter<std::string_view> {
    auto format(op_type t, format_context& ctx) const {
        auto name = [](auto t) {
            switch (t) {
            case op_type::copy:
                return "copy";
            case op_type::append_fragments:
                return "append_fragments";
            case op_type::moves:
                return "moves";
            case op_type::clear:
                return "clear";
            case op_type::print:
                return "print";
            case op_type::iobuf_as_scattered:
                return "iobuf_as_scattered";
            case op_type::hexdump:
                return "hexdump";
            case op_type::trim_front:
                return "trim_front";
            case op_type::trim_back:
                return "trim_back";
            case op_type::prepend_iobuf:
                return "prepend_iobuf";
            case op_type::reserve_memory:
                return "reserve_memory";
            case op_type::prepend_temporary_buffer:
                return "prepend_temporary_buffer";
            case op_type::append_iobuf:
                return "append_iobuf";
            case op_type::append_temporary_buffer:
                return "append_temporary_buffer";
            case op_type::append_uint8_array:
                return "append_uint8_array";
            case op_type::append_char_array:
                return "append_char_array";
            }
        }(t);
        return formatter<std::string_view>::format(name, ctx);
    }
};

/*
 * a program is a series of bytes. the structure of a program is:
 *
 *   [{op_code[, operands...]}*]
 *
 * where the set of operands are dependent on the op_code. the program ends
 * when the end of the program is reached. if the operands for the final
 * op_code are truncated then the program should terminate normally.
 */
class driver {
public:
    explicit driver(std::string_view program)
      : program_(program)
      , pc_(program.cbegin()) {}

    void print_trace() const {
        for (auto op : trace_) {
            fmt::print("TRACED OP: {}\n", op);
        }
    }

    bool operator()() {
        const auto op = [this]() -> std::optional<op_spec> {
            try {
                return next();
            } catch (const end_of_program&) {
                return std::nullopt;
            }
        }();

        if (op.has_value()) {
            handle_op(op.value());
            return true;
        }

        return false;
    }

    void check() const { m_.check(); }

private:
    struct op_spec {
        op_type op;
        std::optional<std::string_view::difference_type> size;
        std::string_view data;

        explicit op_spec(op_type op)
          : op(op) {}

        friend std::ostream& operator<<(std::ostream& os, const op_spec& op) {
            fmt::print(
              os,
              "{}(size={}, data.size={})",
              op.op,
              (op.size.has_value() ? fmt::format("{}", *op.size) : "null"),
              op.data.size());
            return os;
        }
    };

    void handle_op(op_spec op) {
        switch (op.op) {
        case op_type::copy:
            m_.copy();
            return;

        case op_type::moves:
            m_.moves();
            return;

        case op_type::clear:
            m_.clear();
            return;

        case op_type::print:
            m_.print();
            return;

        case op_type::iobuf_as_scattered:
            m_.iobuf_as_scattered();
            return;

        default:
            break;
        }

        vassert(op.size.has_value(), "Op {} requires size operands", op.op);

        switch (op.op) {
        case op_type::append_fragments:
            m_.append_fragments(op.data);
            return;

        case op_type::trim_front:
            m_.trim_front(*op.size);
            return;

        case op_type::trim_back:
            m_.trim_back(*op.size);
            return;

        case op_type::hexdump:
            m_.hexdump(*op.size);
            return;

        case op_type::prepend_iobuf:
            m_.prepend_iobuf(op.data);
            return;

        case op_type::reserve_memory:
            m_.reserve_memory(*op.size);
            return;

        case op_type::prepend_temporary_buffer:
            m_.prepend_temporary_buffer(op.data);
            return;

        case op_type::append_iobuf:
            m_.append_iobuf(op.data);
            return;

        case op_type::append_temporary_buffer:
            m_.append_temporary_buffer(op.data);
            return;

        case op_type::append_uint8_array:
            m_.append_uint8_array(op.data);
            return;

        case op_type::append_char_array:
            m_.append_char_array(op.data);
            return;

        default:
            vassert(false, "Caught wild op {}", op.op);
        }
    }

    // signal that the program should terminate normally
    class end_of_program : std::exception {};

    template<typename T>
    T read() {
        if (std::distance(pc_, program_.cend()) < sizeof(T)) {
            throw end_of_program();
        }
        T ret;
        std::memcpy(&ret, pc_, sizeof(T));
        std::advance(pc_, sizeof(T));
        return ret;
    }

    op_spec next() {
        // next byte is the op code
        using underlying = std::underlying_type_t<op_type>;
        const auto max_op = static_cast<underlying>(op_type::max);
        auto& op = trace_.emplace_back(
          static_cast<op_type>(read<underlying>() % (max_op + 1)));

        // size operand
        switch (op.op) {
        case op_type::trim_front:
        case op_type::trim_back:
        case op_type::prepend_iobuf:
        case op_type::reserve_memory:
        case op_type::hexdump:
        case op_type::prepend_temporary_buffer:
        case op_type::append_fragments:
        case op_type::append_iobuf:
        case op_type::append_temporary_buffer:
        case op_type::append_uint8_array:
        case op_type::append_char_array: {
            op.size = read<uint32_t>();
            break;
        }
        default:
            break;
        }

        // data operand
        switch (op.op) {
        case op_type::append_fragments:
        case op_type::prepend_iobuf:
        case op_type::prepend_temporary_buffer:
        case op_type::append_iobuf:
        case op_type::append_temporary_buffer:
        case op_type::append_uint8_array:
        case op_type::append_char_array:
            vassert(op.size.has_value(), "");
            op.data = {
              pc_,
              std::min<size_t>(*op.size, std::distance(pc_, program_.end()))};
            op.size = op.data.size();
            std::advance(pc_, op.data.size());
            break;

        default:
            break;
        }

        return op;
    }

    const std::string_view program_;
    std::string_view::iterator pc_;
    std::vector<op_spec> trace_;
    iobuf_ops m_;
};

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size < sizeof(std::underlying_type_t<op_type>)) {
        return 0;
    }

    // NOLINTNEXTLINE
    std::string_view d(reinterpret_cast<const char*>(data), size);
    driver p(d);

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
