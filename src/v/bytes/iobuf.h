#pragma once
#include "likely.h"
#include "seastarx.h"
#include "utils/concepts-enabled.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/deleter.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/scattered_message.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/temporary_buffer.hh>

#include <list>
#include <stdexcept>
#include <streambuf>
#include <type_traits>

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
    // 24 of ss::temporary_buffer<> + 8 for capacity tracking

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
    class iterator_consumer;
    class byte_iterator;
    class placeholder;
    struct control {
        container frags;
        size_t size{0};
        allocation_size alloc_sz;
    };
    using control_ptr = control*;
    struct control_allocation {
        control_ptr ctrl;
        ss::deleter del;
    };
    static control_allocation allocate_control();

    iobuf();
    iobuf(control_ptr, ss::deleter) noexcept;
    ~iobuf() = default;
    iobuf(iobuf&& x) noexcept = default;
    iobuf(const iobuf&) = delete;
    iobuf& operator=(const iobuf&) = delete;
    iobuf& operator=(iobuf&& x) noexcept = default;

    /// override to pass in any container of temp bufs
    template<
      typename Range,
      // clang-format off
      typename = std::enable_if<
        std::is_same_v<typename Range::value_type, fragment> ||
        std::is_same_v<typename Range::value_type, ss::temporary_buffer<char>>,
        Range>
      // clang-format on
      >
    explicit iobuf(Range&& r) {
        static_assert(
          std::is_rvalue_reference_v<decltype(r)>,
          "Must be an rvalue. Use std::move()");
        auto alloc = allocate_control();
        _ctrl = alloc.ctrl;
        _deleter = std::move(alloc.del);
        using value_t = typename Range::value_type;
        std::for_each(std::begin(r), std::end(r), [this](value_t& b) {
            append(std::move(b));
        });
    }

    /// makes no stability claims. pre-pending, or appending
    /// might relocate buffers at will. It is intended for read only
    iobuf control_share();
    /// shares _individual_ fragments, not the top level structure
    /// it allocates a _new_ control structure
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
    void append(ss::temporary_buffer<char>);
    /// appends the contents of buffer; might pack values into existing space
    void append(iobuf);
    /// prepends the _the buffer_ as iobuf::fragment::full{}
    void prepend(ss::temporary_buffer<char>);

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
    ss::deleter _deleter;
};

namespace details {
static void check_out_of_range(size_t sz, size_t capacity) {
    if (unlikely(sz > capacity)) {
        throw std::out_of_range("iobuf op: size > capacity");
    }
}
} // namespace details

inline iobuf::control_allocation iobuf::allocate_control() {
    auto data = std::make_unique<iobuf::control>();
    iobuf::control_ptr raw = data.get();
    return control_allocation{raw,
                              ss::make_deleter([data = std::move(data)] {})};
}
class iobuf::fragment {
public:
    struct full {};
    struct empty {};
    fragment(ss::temporary_buffer<char> buf, full)
      : _buf(std::move(buf))
      , _used_bytes(_buf.size()) {}
    fragment(ss::temporary_buffer<char> buf, empty)
      : _buf(std::move(buf))
      , _used_bytes(0) {}
    fragment(fragment&& o) noexcept = default;
    fragment& operator=(fragment&& o) noexcept = default;
    fragment(const fragment& o) = delete;
    fragment& operator=(const fragment& o) = delete;

    ~fragment() = default;

    bool operator==(const fragment& o) const {
        return _used_bytes == o._used_bytes && _buf == o._buf;
    }
    bool operator!=(const fragment& o) const { return !(*this == o); }
    bool is_empty() const { return _used_bytes == 0; }
    size_t available_bytes() const { return _buf.size() - _used_bytes; }
    void reserve(size_t reservation) {
        details::check_out_of_range(reservation, available_bytes());
        _used_bytes += reservation;
    }
    size_t size() const { return _used_bytes; }
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
    ss::temporary_buffer<char> share() {
        // needed for output_stream<char> wrapper
        return _buf.share(0, _used_bytes);
    }
    ss::temporary_buffer<char> share(size_t pos, size_t len) {
        return _buf.share(pos, len);
    }

    /// destructive move. place special care when calling this method
    /// on a shared iobuf. most of the time you want share() instead of release
    ss::temporary_buffer<char> release() && {
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
            _buf = std::move(
              ss::temporary_buffer<char>(_buf.get(), _used_bytes));
        } else {
            _buf.trim(_used_bytes);
        }
    }
    void trim_front(size_t pos) {
        // required by input_stream<char> converter
        _buf.trim_front(pos);
    }

private:
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    char* get_current() { return _buf.get_write() + _used_bytes; }

    friend iobuf::placeholder;

    ss::temporary_buffer<char> _buf;
    size_t _used_bytes;
};

class iobuf::placeholder {
public:
    using iterator = iobuf::iterator;

    placeholder() noexcept = default;

    placeholder(iterator iter, size_t initial_index, size_t max_size_to_write)
      : _iter(iter)
      , _byte_index(initial_index)
      , _remaining_size(max_size_to_write) {}

    [[gnu::always_inline]] void write(const char* src, size_t len) {
        details::check_out_of_range(len, _remaining_size);
        std::copy_n(src, len, mutable_index());
        _remaining_size -= len;
        _byte_index += len;
    }

    size_t remaining_size() const { return _remaining_size; }

    // the first byte of the _current_ iterator + offset
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    const char* index() const { return _iter->_buf.get() + _byte_index; }

private:
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    char* mutable_index() { return _iter->_buf.get_write() + _byte_index; }

    iterator _iter;
    size_t _byte_index{0};
    size_t _remaining_size{0};
};
class iobuf::byte_iterator {
public:
    // iterator_traits
    using difference_type = void;
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
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
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
      , _frag_index_end(frag_index_end) {}

    pointer get() const { return _frag_index; }
    reference operator*() const noexcept { return *_frag_index; }
    pointer operator->() const noexcept { return _frag_index; }
    /// true if pointing to the byte-value (not necessarily the same address)
    bool operator==(const byte_iterator& o) const noexcept {
        return _frag_index == o._frag_index;
    }
    bool operator!=(const byte_iterator& o) const noexcept {
        return !(*this == o);
    }
    byte_iterator& operator++() {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        if (++_frag_index == _frag_index_end) {
            next_fragment();
        }
        return *this;
    }

private:
    void next_fragment() {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        if (++_frag != _frag_end) {
            _frag_index = _frag->get();
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
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
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            _frag_index_end = _frag->get() + _frag->size();
        }
    }
    void skip(size_t n) {
        size_t c = consume(n, [](const char*, size_t /*max*/) {
            return ss::stop_iteration::no;
        });
        if (unlikely(c != n)) {
            throw std::out_of_range("Invalid skip(n)");
        }
    }
    template<typename Output>
    [[gnu::always_inline]] void consume_to(size_t n, Output out) {
        size_t c = consume(n, [&out](const char* src, size_t max) {
            std::copy_n(src, max, out);
            out += max;
            return ss::stop_iteration::no;
        });
        if (unlikely(c != n)) {
            throw std::out_of_range("Invalid consume_to(n, out)");
        }
    }

    template<
      typename T,
      typename = std::enable_if_t<std::is_trivially_copyable_v<T>, T>>
    T consume_type() {
        constexpr size_t sz = sizeof(T);
        T obj;
        char* dst = reinterpret_cast<char*>(&obj); // NOLINT
        consume_to(sz, dst);
        return obj;
    }
    template<
      typename T,
      typename = std::enable_if_t<std::is_arithmetic_v<T>, T>>
    T consume_be_type() {
        return ss::be_to_cpu(consume_type<T>());
    }
    [[gnu::always_inline]] void consume_to(size_t n, iobuf::placeholder& ph) {
        size_t c = consume(n, [&ph](const char* src, size_t max) {
            ph.write(src, max);
            return ss::stop_iteration::no;
        });
        if (unlikely(c != n)) {
            throw std::out_of_range("Invalid consume_to(n, placeholder)");
        }
    }

    template<typename Consumer>
    // clang-format off
    CONCEPT(requires requires(Consumer c, const char* src, size_t max) {
                    { c(src, max) } -> ss::stop_iteration;
            }
    )
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
                // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
                if (++_frag != _frag_end) {
                    _frag_index = _frag->get();
                    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
                    _frag_index_end = _frag->get() + _frag->size();
                }
                continue;
            }
            const size_t step = std::min(n - i, bytes_left);
            const ss::stop_iteration stop = f(_frag_index, step);
            i += step;
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            _frag_index += step;
            _bytes_consumed += step;
            if (stop == ss::stop_iteration::yes) {
                break;
            }
        }

        if (_frag_index == _frag_index_end) {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            if (_frag != _frag_end && ++_frag != _frag_end) {
                _frag_index = _frag->get();
                // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
                _frag_index_end = _frag->get() + _frag->size();
            } else {
                _frag_index = nullptr;
                _frag_index_end = nullptr;
            }
        }

        return i;
    }
    size_t bytes_consumed() const { return _bytes_consumed; }
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    size_t segment_bytes_left() const { return _frag_index_end - _frag_index; }
    bool is_finished() const { return _frag == _frag_end; }

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
    iobuf::const_iterator _frag;
    iobuf::const_iterator _frag_end;
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

inline iobuf::iobuf() {
    auto alloc = allocate_control();
    _ctrl = alloc.ctrl;
    _deleter = std::move(alloc.del);
}
/// constructor used for sharing
inline iobuf::iobuf(iobuf::control_ptr c, ss::deleter del) noexcept
  : _ctrl(c)
  , _deleter(std::move(del)) {}

inline iobuf::iterator iobuf::begin() { return _ctrl->frags.begin(); }
inline iobuf::iterator iobuf::end() { return _ctrl->frags.end(); }
inline iobuf::const_iterator iobuf::begin() const {
    return _ctrl->frags.cbegin();
}
inline iobuf::const_iterator iobuf::end() const { return _ctrl->frags.cend(); }
inline iobuf::const_iterator iobuf::cbegin() const {
    return _ctrl->frags.cbegin();
}
inline iobuf::const_iterator iobuf::cend() const { return _ctrl->frags.cend(); }

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
inline bool iobuf::operator!=(const iobuf& o) const { return !(*this == o); }
inline bool iobuf::empty() const { return _ctrl->frags.empty(); }
inline size_t iobuf::size_bytes() const { return _ctrl->size; }

inline size_t iobuf::available_bytes() const {
    if (_ctrl->frags.empty()) {
        return 0;
    }
    return _ctrl->frags.back().available_bytes();
}

inline iobuf iobuf::control_share() { return iobuf(_ctrl, _deleter.share()); }

inline iobuf iobuf::share(size_t pos, size_t len) {
    auto alloc = allocate_control();
    auto c = alloc.ctrl;
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
    return iobuf(c, std::move(alloc.del));
}

inline iobuf iobuf::copy() const {
    // make sure we pass in our learned allocation strategy _ctrl->alloc_sz
    auto alloc = allocate_control();
    iobuf ret(alloc.ctrl, std::move(alloc.del));
    ret._ctrl->alloc_sz = _ctrl->alloc_sz;
    auto in = iobuf::iterator_consumer(cbegin(), cend());
    in.consume(_ctrl->size, [&ret](const char* src, size_t sz) {
        ret.append(src, sz);
        return ss::stop_iteration::no;
    });
    return ret;
}
inline void iobuf::create_new_fragment(size_t sz) {
    auto asz = _ctrl->alloc_sz.next_allocation_size(sz);
    _ctrl->frags.push_back(iobuf::fragment(
      ss::temporary_buffer<char>(asz), iobuf::fragment::empty{}));
}
inline iobuf::placeholder iobuf::reserve(size_t sz) {
    if (auto b = available_bytes(); b < sz) {
        if (b > 0) {
            _ctrl->frags.back().trim();
        }
        create_new_fragment(sz); // make space if not enough
    }
    _ctrl->size += sz;
    auto it = std::prev(_ctrl->frags.end());
    placeholder p(it, it->size(), sz);
    it->reserve(sz);
    return p;
}

[[gnu::always_inline]] void inline iobuf::prepend(
  ss::temporary_buffer<char> b) {
    _ctrl->size += b.size();
    _ctrl->frags.emplace_front(std::move(b), iobuf::fragment::full{});
}
[[gnu::always_inline]] void inline iobuf::append(const char* ptr, size_t size) {
    if (size == 0) {
        return;
    }
    if (likely(size <= available_bytes())) {
        _ctrl->size += _ctrl->frags.back().append(ptr, size);
        return;
    }
    size_t i = 0;
    while (size > 0) {
        if (available_bytes() == 0) {
            create_new_fragment(size);
        }
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        const size_t sz = _ctrl->frags.back().append(ptr + i, size);
        _ctrl->size += sz;
        i += sz;
        size -= sz;
    }
}

/// appends the contents of buffer; might pack values into existing space
[[gnu::always_inline]] inline void iobuf::append(ss::temporary_buffer<char> b) {
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
        append(f.share());
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

inline ss::input_stream<char> make_iobuf_input_stream(iobuf io) {
    struct iobuf_input_stream final : ss::data_source_impl {
        explicit iobuf_input_stream(iobuf i)
          : io(std::move(i)) {}
        ss::future<ss::temporary_buffer<char>> skip(uint64_t n) final {
            io.trim_front(n);
            return get();
        }
        ss::future<ss::temporary_buffer<char>> get() final {
            if (io.begin() == io.end()) {
                return ss::make_ready_future<ss::temporary_buffer<char>>();
            }
            auto buf = io.begin()->share();
            io.pop_front();
            return ss::make_ready_future<ss::temporary_buffer<char>>(
              std::move(buf));
        }
        iobuf io;
    };
    auto ds = ss::data_source(
      std::make_unique<iobuf_input_stream>(std::move(io)));
    return ss::input_stream<char>(std::move(ds));
}
inline ss::output_stream<char> make_iobuf_output_stream(iobuf io) {
    struct iobuf_output_stream final : ss::data_sink_impl {
        explicit iobuf_output_stream(iobuf i)
          : io(std::move(i)) {}
        ss::future<> put(ss::net::packet data) final {
            auto all = data.release();
            for (auto& b : all) {
                io.append(std::move(b));
            }
            return ss::make_ready_future<>();
        }
        ss::future<> put(std::vector<ss::temporary_buffer<char>> all) final {
            for (auto& b : all) {
                io.append(std::move(b));
            }
            return ss::make_ready_future<>();
        }
        ss::future<> put(ss::temporary_buffer<char> buf) final {
            io.append(std::move(buf));
            return ss::make_ready_future<>();
        }
        ss::future<> flush() final { return ss::make_ready_future<>(); }
        ss::future<> close() final { return ss::make_ready_future<>(); }
        iobuf io;
    };
    const size_t sz = io.size_bytes();
    return ss::output_stream<char>(
      ss::data_sink(std::make_unique<iobuf_output_stream>(std::move(io))), sz);
}

inline ss::future<iobuf>
read_iobuf_exactly(ss::input_stream<char>& in, size_t n) {
    return ss::do_with(iobuf(), n, [&in](iobuf& b, size_t& n) {
        return ss::do_until(
                 [&n] { return n == 0; },
                 [&n, &in, &b] {
                     if (n == 0) {
                         return ss::make_ready_future<>();
                     }
                     return in.read_up_to(n).then(
                       [&n, &b](ss::temporary_buffer<char> buf) {
                           if (buf.empty()) {
                               n = 0;
                               return;
                           }
                           n -= buf.size();
                           b.append(std::move(buf));
                       });
                 })
          .then([&b] { return ss::make_ready_future<iobuf>(std::move(b)); });
    });
}

inline std::vector<iobuf> iobuf_share_foreign_n(iobuf&& og, size_t n) {
    const auto shard = ss::this_shard_id();
    std::vector<iobuf> retval(n);
    for (auto& frag : og) {
        auto tmpbuf = std::move(frag).release();
        char* src = tmpbuf.get_write();
        const size_t sz = tmpbuf.size();
        ss::deleter del = tmpbuf.release();
        for (iobuf& b : retval) {
            ss::deleter del_i = ss::make_deleter(
              [shard, d = del.share()]() mutable {
                  (void)ss::smp::submit_to(shard, [d = std::move(d)] {});
              });
            b.append(ss::temporary_buffer<char>(src, sz, std::move(del_i)));
        }
    }
    return retval;
}

static inline ss::scattered_message<char> iobuf_as_scattered(iobuf b) {
    ss::scattered_message<char> msg;
    auto in = iobuf::iterator_consumer(b.cbegin(), b.cend());
    in.consume(b.size_bytes(), [&msg](const char* src, size_t sz) {
        msg.append_static(src, sz);
        return ss::stop_iteration::no;
    });
    msg.on_delete([b = std::move(b)] {});
    return msg;
}

inline std::ostream& operator<<(std::ostream& o, const iobuf& io) {
    return o << "{bytes=" << io.size_bytes()
             << ", fragments=" << std::distance(io.cbegin(), io.cend()) << "}";
}

/// A simple ostream buffer appender. No other op is currently supported.
/// Currently works with char-by-char iterators as well. See iobuf_tests.cc
///
/// iobuf underlying;
/// iobuf_ostreambuf obuf(underlying);
/// std::ostream os(&obuf);
///
/// os << "hello world";
///
class iobuf_ostreambuf final : public std::streambuf {
public:
    explicit iobuf_ostreambuf(iobuf& o)
      : _buf(o.control_share()) {}
    iobuf_ostreambuf(iobuf_ostreambuf&&) noexcept = default;
    iobuf_ostreambuf& operator=(iobuf_ostreambuf&&) noexcept = default;
    iobuf_ostreambuf(const iobuf_ostreambuf&) = delete;
    iobuf_ostreambuf& operator=(const iobuf_ostreambuf&) = delete;
    int_type overflow(int_type c = traits_type::eof()) final {
        if (c == traits_type::eof()) {
            return traits_type::eof();
        }
        char_type ch = traits_type::to_char_type(c);
        return xsputn(&ch, 1) == 1 ? c : traits_type::eof();
    }
    std::streamsize xsputn(const char_type* s, std::streamsize n) final {
        _buf.append(s, n);
        return n;
    }
    ~iobuf_ostreambuf() override = default;

private:
    iobuf _buf;
};
