#pragma once

#include "seastarx.h"
#include "utils/concepts-enabled.h"

#include <seastar/core/do_with.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>

#include <list>
#include <stdexcept>

/// our iobuf is a fragmented buffer. modeled after
/// folly::iobufqueue.h - it supports prepend and append, but no
/// operations in the middle. It provides a forward iterator for
/// byte scanning and parsing. This is intended to be the workhorse
/// of our data path.
/// Operations:
/// Append      - ammortized O(1); O(N) when vector at capacity; N== # fragments
/// operator==  - O(N) - N == bytes
/// operator!=  - O(N) - N == bytes
class iobuf {
    // Not a lightweight object.
    // 24 bytes for std::list
    // 8  bytes for _ctrl->alloc_sz
    // 8  bytes for _size_bytes
    // -----------------------
    //
    // 40 bytes total.

    // 32 bytes per fragment
    // 24 of temporary_buffer<> + 8 for capacity tracking

public:
    class allocation_size {
    public:
        static constexpr size_t max_chunk_size = 128 * 1024;
        static constexpr size_t default_chunk_size = 512;
        size_t next_allocation_size(size_t data_size);
        size_t current_allocation_size() const;
        void reset();

    private:
        size_t _next_alloc_sz{default_chunk_size};
    };
    class fragment;
    using container = std::list<fragment>;
    using iterator = typename container::iterator;
    using const_iterator = typename container::const_iterator;
    using reverse_iterator = typename container::reverse_iterator;
    class iterator_consumer;
    class byte_iterator;
    class placeholder;
    struct control {
        container frags;
        size_t size{0};
        allocation_size alloc_sz;
    };
    using control_ptr = lw_shared_ptr<control>;

    iobuf() noexcept;
    iobuf(control_ptr) noexcept;
    ~iobuf() = default;
    iobuf(iobuf&& x) noexcept = default;
    iobuf& operator=(iobuf&& x) noexcept = default;

    /// override to pass in any container of temp bufs
    template<
      typename Range,
      // clang-format off
      typename = std::enable_if<
        std::is_same_v<typename Range::value_type, fragment> ||
        std::is_same_v<typename Range::value_type, temporary_buffer<char>>,
        Range>
      // clang-format on
      >
    iobuf(Range&& r)
      : _ctrl(make_lw_shared<control>()) {
        static_assert(
          std::is_rvalue_reference_v<decltype(r)>,
          "Must be an rvalue. Use std::move()");
        using value_t = typename Range::value_type;
        std::for_each(std::begin(r), std::end(r), [this](value_t& b) {
            append(std::move(b));
        });
    }

    /// makes no stability claims. pre-pending, or appending
    /// might relocate buffers at will. It is intended for read only
    iobuf share();
    /// shares _individual_ fragments, not the top level structure
    iobuf share(size_t pos, size_t len);
    /// makes a copy of the data
    iobuf copy() const;

    /// makes a reservation with the internal storage. adds a layer of
    /// indirection instead of raw byte pointer to allow the fragments to
    /// internally compact buffers as long as they don't violate
    /// the reservation size here
    placeholder reserve(size_t reservation);

    /// append src + len into storage
    void append(const char*, size_t);
    /// appends the contents of buffer; might pack values into existing space
    void append(fragment);
    /// appends the contents of buffer; might pack values into existing space
    void append(temporary_buffer<char>);
    /// appends the contents of buffer; might pack values into existing space
    void append(iobuf);
    /// prepends the _the buffer_ as iobuf::fragment::full{}
    void prepend(temporary_buffer<char>);

    /// used for iostreams
    void pop_front();
    void trim_front(size_t n);

    size_t size_bytes() const;
    bool empty() const;
    /// compares that the _content_ is the same;
    /// ignores allocation strategy, and number of fragments
    /// it is a byte-per-byte comparator
    bool operator==(const iobuf&) const;
    bool operator!=(const iobuf&) const;

    iterator begin();
    iterator end();
    const_iterator begin() const;
    const_iterator end() const;
    const_iterator cbegin() const;
    const_iterator cend() const;

private:
    size_t available_bytes() const;
    void create_new_fragment(size_t);

    control_ptr _ctrl;
};

input_stream<char> make_iobuf_input_stream(iobuf);
output_stream<char> make_iobuf_output_stream(iobuf);
future<iobuf> read_exactly(input_stream<char>&, size_t);

// -- implementation details below

namespace details {
static void check_out_of_range(size_t sz, size_t capacity) {
    if (__builtin_expect(sz > capacity, false)) {
        throw std::out_of_range("iobuf op: size > capacity");
    }
}
} // namespace details

class iobuf::fragment {
public:
    struct full {};
    struct empty {};
    fragment(temporary_buffer<char> buf, full)
      : _buf(std::move(buf))
      , _used_bytes(_buf.size()) {
    }
    fragment(temporary_buffer<char> buf, empty)
      : _buf(std::move(buf))
      , _used_bytes(0) {
    }
    fragment(fragment&& o) noexcept = default;
    fragment& operator=(fragment&& o) noexcept = default;
    ~fragment() = default;

    bool operator==(const fragment& o) const {
        return _used_bytes == o._used_bytes && _buf == o._buf;
    }
    bool operator!=(const fragment& o) const {
        return !(*this == o);
    }
    bool is_empty() const {
        return _used_bytes == 0;
    }
    size_t available_bytes() const {
        return _buf.size() - _used_bytes;
    }
    void reserve(size_t reservation) {
        details::check_out_of_range(reservation, available_bytes());
        _used_bytes += reservation;
    }
    size_t size() const {
        return _used_bytes;
    }
    const char* get() const {
        // required for the networking layer to conver to
        // scattered message without copying data
        return _buf.get();
    }
    size_t append(const char* src, size_t len) {
        const size_t sz = std::min(len, available_bytes());
        std::copy_n(src, sz, get_current());
        _used_bytes += sz;
        return sz;
    }
    temporary_buffer<char> share() {
        // needed for output_stream<char> wrapper
        return _buf.share(0, _used_bytes);
    }
    temporary_buffer<char> share(size_t pos, size_t len) {
        return _buf.share(0, len);
    }
    temporary_buffer<char> release() && {
        trim();
        return std::move(_buf);
    }
    void trim() {
        if (_used_bytes == _buf.size()) {
            return;
        }
        size_t half = _buf.size() / 2;
        if (_used_bytes <= half) {
            // this is an important optimization. often times during RPC
            // serialization we append some small controll bytes, _right_
            // before we append a full new chain of iobufs
            _buf = std::move(temporary_buffer<char>(_buf.get(), _used_bytes));
        } else {
            _buf.trim(_used_bytes);
        }
    }
    void trim_front(size_t pos) {
        // required by input_stream<char> converter
        _buf.trim_front(pos);
    }

private:
    char* get_current() {
        return _buf.get_write() + _used_bytes;
    }

    friend iobuf::placeholder;

    temporary_buffer<char> _buf;
    size_t _used_bytes;
};

class iobuf::placeholder {
public:
    using iterator = iobuf::reverse_iterator;

    placeholder() noexcept {};
    placeholder(iterator iter, size_t initial_index, size_t max_size_to_write)
      : _iter(iter)
      , _byte_index(initial_index)
      , _remaining_size(max_size_to_write) {
    }

    [[gnu::always_inline]] void write(const char* src, size_t len) {
        details::check_out_of_range(len, _remaining_size);
        std::copy_n(src, len, mutable_index());
        _remaining_size -= len;
        _byte_index += len;
    }

    size_t remaining_size() const {
        return _remaining_size;
    }

    // the first byte of the _current_ iterator + offset
    const char* index() const {
        return _iter->_buf.get() + _byte_index;
    }

private:
    char* mutable_index() {
        return _iter->_buf.get_write() + _byte_index;
    }

    iterator _iter;
    size_t _byte_index{0};
    size_t _remaining_size{0};
};
class iobuf::byte_iterator {
public:
    // iterator_traits
    using value_type = char;
    using pointer = const char*;
    using reference = const char&;
    using iterator_category = std::forward_iterator_tag;

    byte_iterator(
      iobuf::const_iterator begin, iobuf::const_iterator end) noexcept
      : _frag(begin)
      , _frag_end(end) {
        if (_frag != _frag_end) {
            _frag_index = _frag->get();
            _frag_index_end = _frag->get() + _frag->size();
        } else {
            _frag_index = nullptr;
            _frag_index_end = nullptr;
        }
    }
    byte_iterator(
      iobuf::const_iterator begin,
      iobuf::const_iterator end,
      const char* frag_index,
      const char* frag_index_end) noexcept
      : _frag(begin)
      , _frag_end(end)
      , _frag_index(frag_index)
      , _frag_index_end(frag_index_end) {
    }

    pointer get() const {
        return _frag_index;
    }
    reference operator*() const noexcept {
        return *_frag_index;
    }
    pointer operator->() const noexcept {
        return _frag_index;
    }
    /// true if pointing to the byte-value (not necessarily the same address)
    bool operator==(const byte_iterator& o) const noexcept {
        return _frag_index == o._frag_index;
    }
    bool operator!=(const byte_iterator& o) const noexcept {
        return !(*this == o);
    }
    byte_iterator& operator++() {
        if (++_frag_index == _frag_index_end) {
            next_fragment();
        }
        return *this;
    }

private:
    void next_fragment() {
        if (++_frag != _frag_end) {
            _frag_index = _frag->get();
            _frag_index_end = _frag->get() + _frag->size();
        } else {
            _frag_index = nullptr;
            _frag_index_end = nullptr;
        }
    }

    iobuf::const_iterator _frag_end;
    iobuf::const_iterator _frag;
    const char* _frag_index = nullptr;
    const char* _frag_index_end = nullptr;
};
class iobuf::iterator_consumer {
public:
    iterator_consumer(
      iobuf::const_iterator begin, iobuf::const_iterator end) noexcept
      : _frag(begin)
      , _frag_end(end) {
        if (_frag != _frag_end) {
            _frag_index = _frag->get();
            _frag_index_end = _frag->get() + _frag->size();
        }
    }
    void skip(size_t n) {
        size_t c = consume(
          n, [](const char*, size_t max) { return stop_iteration::no; });
        if (__builtin_expect(c != n, false)) {
            throw std::out_of_range("Invalid skip(n)");
        }
    }
    template<typename Output>
    [[gnu::always_inline]] void consume_to(size_t n, Output out) {
        size_t c = consume(n, [&out](const char* src, size_t max) {
            std::copy_n(src, max, out);
            out += max;
            return stop_iteration::no;
        });
        if (__builtin_expect(c != n, false)) {
            throw std::out_of_range("Invalid consume_to(n, out)");
        }
    }

    template<typename T>
    T consume_type() {
        constexpr size_t sz = sizeof(T);
        T obj;
        char* dst = reinterpret_cast<char*>(&obj);
        consume_to(sz, dst);
        return obj;
    }

    [[gnu::always_inline]] void consume_to(size_t n, iobuf::placeholder& ph) {
        size_t c = consume(n, [&ph](const char* src, size_t max) {
            ph.write(src, max);
            return stop_iteration::no;
        });
        if (__builtin_expect(c != n, false)) {
            throw std::out_of_range("Invalid consume_to(n, placeholder)");
        }
    }

    // clang-format off
    template<typename Consumer>
    CONCEPT(requires requires(Consumer c, const char* src, size_t max) {
        { c(src, max) } -> stop_iteration;
    })
    // clang-format on
    /// takes a Consumer object and iteraters over the chunks in oder, from the
    /// given buffer index position. Use a stop_iteration::yes for early exit;
    size_t consume(const size_t n, Consumer&& f) {
        size_t i = 0;
        while (i < n) {
            if (_frag == _frag_end) {
                return i;
            }
            const size_t bytes_left = segment_bytes_left();
            if (bytes_left == 0) {
                if (++_frag != _frag_end) {
                    _frag_index = _frag->get();
                    _frag_index_end = _frag->get() + _frag->size();
                }
                continue;
            }
            const size_t step = std::min(n - i, bytes_left);
            const stop_iteration stop = f(_frag_index, step);
            i += step;
            _frag_index += step;
            _bytes_consumed += step;
            if (stop == stop_iteration::yes) {
                return i;
            }
        }
        return i;
    }
    size_t bytes_consumed() const {
        return _bytes_consumed;
    }
    size_t segment_bytes_left() const {
        return _frag_index_end - _frag_index;
    }
    bool is_finished() const {
        return _frag == _frag_end;
    }

    /// starts a new iterator byte-for-byte starting at *this* index
    /// useful for varint decoding that need to peek ahead
    iobuf::byte_iterator begin() const {
        return iobuf::byte_iterator(
          _frag, _frag_end, _frag_index, _frag_index_end);
    }
    iobuf::byte_iterator end() const {
        return iobuf::byte_iterator(_frag_end, _frag_end, nullptr, nullptr);
    }

private:
    iobuf::const_iterator _frag_end;
    iobuf::const_iterator _frag;
    const char* _frag_index = nullptr;
    const char* _frag_index_end = nullptr;
    size_t _bytes_consumed{0};
};

//   - try to not exceed max_chunk_size
//   - must be enough for data_size
//   - uses folly::vector of 1.5 growth without using double conversions
inline size_t iobuf::allocation_size::next_allocation_size(size_t data_size) {
    size_t next_size = ((_next_alloc_sz * 3) + 1) / 2;
    next_size = std::min(next_size, max_chunk_size);
    return _next_alloc_sz = std::max(next_size, data_size);
}
inline size_t iobuf::allocation_size::current_allocation_size() const {
    return _next_alloc_sz;
}
inline void iobuf::allocation_size::reset() {
    _next_alloc_sz = default_chunk_size;
}

inline iobuf::iobuf() noexcept
  : _ctrl(make_lw_shared<iobuf::control>()) {
}
/// constructor used for sharing
inline iobuf::iobuf(iobuf::control_ptr c) noexcept
  : _ctrl(c) {
}

inline iobuf::iterator iobuf::begin() {
    return _ctrl->frags.begin();
}
inline iobuf::iterator iobuf::end() {
    return _ctrl->frags.end();
}
inline iobuf::const_iterator iobuf::begin() const {
    return _ctrl->frags.cbegin();
}
inline iobuf::const_iterator iobuf::end() const {
    return _ctrl->frags.cend();
}
inline iobuf::const_iterator iobuf::cbegin() const {
    return _ctrl->frags.cbegin();
}
inline iobuf::const_iterator iobuf::cend() const {
    return _ctrl->frags.cend();
}

inline bool iobuf::operator==(const iobuf& o) const {
    if (_ctrl->size != o._ctrl->size) {
        return false;
    }
    auto lhs_begin = iobuf::byte_iterator(cbegin(), cend());
    auto lhs_end = iobuf::byte_iterator(cend(), cend());
    auto rhs = iobuf::byte_iterator(o.cbegin(), o.cend());
    while (lhs_begin != lhs_end) {
        char l = *lhs_begin;
        char r = *rhs;
        if (l != r) {
            return false;
        }
        ++lhs_begin;
        ++rhs;
    }
    return true;
}
inline bool iobuf::operator!=(const iobuf& o) const {
    return !(*this == o);
}
inline bool iobuf::empty() const {
    return _ctrl->frags.empty();
}
inline size_t iobuf::size_bytes() const {
    return _ctrl->size;
}

inline size_t iobuf::available_bytes() const {
    if (_ctrl->frags.empty()) {
        return 0;
    }
    return _ctrl->frags.back().available_bytes();
}

inline iobuf iobuf::share() {
    return iobuf(_ctrl);
}

inline iobuf iobuf::share(size_t pos, size_t len) {
    auto c = make_lw_shared<iobuf::control>();
    c->size = len;
    c->alloc_sz = _ctrl->alloc_sz;
    size_t left = len;
    for (auto& frag : _ctrl->frags) {
        if (left == 0) {
            break;
        }
        if (pos >= frag.size()) {
            pos -= frag.size();
            continue;
        }
        size_t left_in_frag = frag.size() - pos;
        if (left >= left_in_frag) {
            left -= left_in_frag;
        } else {
            left_in_frag = left;
            left = 0;
        }
        c->frags.emplace_back(frag.share(pos, left_in_frag), fragment::full{});
        pos = 0;
    }
    return iobuf(c);
}

inline iobuf iobuf::copy() const {
    // make sure we pass in our learned allocation strategy _ctrl->alloc_sz
    iobuf ret(make_lw_shared<iobuf::control>());
    ret._ctrl->alloc_sz = _ctrl->alloc_sz;
    auto in = iobuf::iterator_consumer(cbegin(), cend());
    in.consume(_ctrl->size, [&ret](const char* src, size_t sz) {
        ret.append(src, sz);
        return stop_iteration::no;
    });
    return ret;
}
inline void iobuf::create_new_fragment(size_t sz) {
    auto asz = _ctrl->alloc_sz.next_allocation_size(sz);
    _ctrl->frags.push_back(
      iobuf::fragment(temporary_buffer<char>(asz), iobuf::fragment::empty{}));
}
inline iobuf::placeholder iobuf::reserve(size_t sz) {
    if (auto b = available_bytes(); b < sz) {
        if (b > 0) {
            _ctrl->frags.back().trim();
        }
        create_new_fragment(sz); // make space if not enough
    }
    _ctrl->size += sz;
    auto it = _ctrl->frags.rbegin();
    placeholder p(it, it->size(), sz);
    it->reserve(sz);
    return std::move(p);
}

[[gnu::always_inline]] void inline iobuf::prepend(temporary_buffer<char> b) {
    _ctrl->size += b.size();
    _ctrl->frags.emplace_front(std::move(b), iobuf::fragment::full{});
}
[[gnu::always_inline]] void inline iobuf::append(const char* ptr, size_t size) {
    if (size == 0) {
        return;
    }
    if (__builtin_expect(size <= available_bytes(), true)) {
        _ctrl->size += _ctrl->frags.back().append(ptr, size);
        return;
    }
    size_t i = 0;
    while (size > 0) {
        if (available_bytes() == 0) {
            create_new_fragment(size);
        }
        const size_t sz = _ctrl->frags.back().append(ptr + i, size);
        _ctrl->size += sz;
        i += sz;
        size -= sz;
    }
}
[[gnu::always_inline]] inline void iobuf::append(iobuf::fragment b) {
    append(std::move(b).release());
}
/// appends the contents of buffer; might pack values into existing space
[[gnu::always_inline]] inline void iobuf::append(temporary_buffer<char> b) {
    if (b.size() <= available_bytes()) {
        append(b.get(), b.size());
        return;
    }
    if (available_bytes() > 0) {
        if (_ctrl->frags.back().is_empty()) {
            _ctrl->frags.pop_back();
        } else {
            // happens when we are merge iobufs
            _ctrl->frags.back().trim();
            _ctrl->alloc_sz.reset();
        }
    }
    _ctrl->size += b.size();
    _ctrl->frags.emplace_back(std::move(b), iobuf::fragment::full{});
}
/// appends the contents of buffer; might pack values into existing space
inline void iobuf::append(iobuf o) {
    for (auto& f : o) {
        append(std::move(f));
    }
}
/// used for iostreams
inline void iobuf::pop_front() {
    _ctrl->size -= _ctrl->frags.front().size();
    _ctrl->frags.pop_front();
}
inline void iobuf::trim_front(size_t n) {
    while (!_ctrl->frags.empty()) {
        auto& f = _ctrl->frags.front();
        if (f.size() > n) {
            _ctrl->size -= n;
            f.trim_front(n);
            return;
        }
        pop_front();
    }
}

inline input_stream<char> make_iobuf_input_stream(iobuf io) {
    struct iobuf_input_stream final : data_source_impl {
        iobuf_input_stream(iobuf i)
          : io(std::move(i)) {
        }
        future<temporary_buffer<char>> skip(uint64_t n) final {
            io.trim_front(n);
            return get();
        }
        future<temporary_buffer<char>> get() final {
            if (io.begin() == io.end()) {
                return make_ready_future<temporary_buffer<char>>();
            }
            auto buf = io.begin()->share();
            io.pop_front();
            return make_ready_future<temporary_buffer<char>>(std::move(buf));
        }
        iobuf io;
    };
    auto ds = data_source(std::make_unique<iobuf_input_stream>(std::move(io)));
    return input_stream<char>(std::move(ds));
}
inline output_stream<char> make_iobuf_output_stream(iobuf io) {
    struct iobuf_output_stream final : data_sink_impl {
        iobuf_output_stream(iobuf i)
          : io(std::move(i)) {
        }
        future<> put(net::packet data) final {
            auto all = data.release();
            for (auto& b : all) {
                io.append(std::move(b));
            }
            return make_ready_future<>();
        }
        future<> put(std::vector<temporary_buffer<char>> all) final {
            for (auto& b : all) {
                io.append(std::move(b));
            }
            return make_ready_future<>();
        }
        future<> put(temporary_buffer<char> buf) final {
            io.append(std::move(buf));
            return make_ready_future<>();
        }
        future<> flush() final {
            return make_ready_future<>();
        }
        future<> close() final {
            return make_ready_future<>();
        }
        iobuf io;
    };
    const size_t sz = io.size_bytes();
    return output_stream<char>(
      data_sink(std::make_unique<iobuf_output_stream>(std::move(io))), sz);
}

inline future<iobuf> read_iobuf_exactly(input_stream<char>& in, size_t n) {
    static constexpr auto max_chunk_size
      = iobuf::allocation_size::max_chunk_size;
    return do_with(iobuf(), n, [&in](iobuf& b, size_t& n) {
        return do_until(
                 [&n] { return n == 0; },
                 [&n, &in, &b] {
                     const auto step = std::min(n, max_chunk_size);
                     if (step == 0) {
                         return make_ready_future<>();
                     }
                     return in.read_up_to(step).then(
                       [&n, &b](temporary_buffer<char> buf) {
                           if (buf.empty()) {
                               n = 0;
                               return;
                           }
                           n -= buf.size();
                           b.append(std::move(buf));
                       });
                 })
          .then([&b] { return make_ready_future<iobuf>(std::move(b)); });
    });
}

inline std::ostream& operator<<(std::ostream& o, const iobuf& io) {
    return o << "{bytes=" << io.size_bytes()
             << ", fragments=" << std::distance(io.cbegin(), io.cend()) << "}";
}
