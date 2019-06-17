#pragma once

#include "bytes/bytes.h"
#include "bytes/bytes_ostream.h"
#include "utils/concepts-enabled.h"
#include "utils/fragment_range.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/simple-stream.hh>
#include <seastar/core/temporary_buffer.hh>

#include <fmt/format.h>

#include <iterator>
#include <vector>

/// Fragmented buffer consisting of multiple temporary_buffer<char>
class fragmented_temporary_buffer {
    using vector_type = std::vector<seastar::temporary_buffer<char>>;

public:
    static constexpr size_t default_fragment_size = 128 * 1024;

    class istream;
    class reader;

    fragmented_temporary_buffer() = default;

    fragmented_temporary_buffer(
      std::vector<seastar::temporary_buffer<char>> fragments,
      size_t size_bytes) noexcept
      : _fragments(std::move(fragments))
      , _size_bytes(size_bytes) {
    }

    istream get_istream() const noexcept;

    size_t size_bytes() const {
        return _size_bytes;
    }

    bool empty() const {
        return !_size_bytes;
    }

private:
    vector_type _fragments;
    size_t _size_bytes = 0;
};

namespace fragmented_temporary_buffer_concepts {
// clang-format off
CONCEPT(
template<typename T>
concept bool ExceptionThrower = requires(T obj, size_t n) {
    obj.throw_out_of_range(n, n);
};
)
// clang-format on
} // namespace fragmented_temporary_buffer_concepts

class fragmented_temporary_buffer::istream {
    void next_fragment() {
        _bytes_left -= _current->size();
        if (_bytes_left) {
            _current++;
            _current_position = _current->begin();
            _current_end = _current->end();
        } else {
            _current_position = nullptr;
            _current_end = nullptr;
        }
    }

    template<typename ExceptionThrower>
    CONCEPT(requires fragmented_temporary_buffer_concepts::ExceptionThrower<
            ExceptionThrower>)
    void check_out_of_range(ExceptionThrower& exceptions, size_t n) {
        if (__builtin_expect(bytes_left() < n, false)) {
            exceptions.throw_out_of_range(n, bytes_left());
        }
    }

    template<typename T, typename ExceptionThrower>
    [[gnu::noinline]] [[gnu::cold]] T read_slow(ExceptionThrower&& exceptions) {
        check_out_of_range(exceptions, sizeof(T));

        T obj;
        size_t left = sizeof(T);
        while (left) {
            auto this_length = std::min<size_t>(
              left, _current_end - _current_position);
            std::copy_n(
              _current_position,
              this_length,
              reinterpret_cast<char*>(&obj) + sizeof(T) - left);
            left -= this_length;
            if (left) {
                next_fragment();
            } else {
                _current_position += this_length;
            }
        }
        return obj;
    }

    template<typename Output, typename ExceptionThrower>
    [[gnu::noinline]] [[gnu::cold]] Output read_to_slow(
      size_t n,
      Output out,
      ExceptionThrower&& exceptions = default_exception_thrower()) {
        check_out_of_range(exceptions, n);
        out = std::copy(_current_position, _current_end, out);
        n -= _current_end - _current_position;
        next_fragment();
        while (n > _current->size()) {
            out = std::copy(_current_position, _current_end, out);
            n -= _current->size();
            next_fragment();
        }
        out = std::copy_n(_current_position, n, out);
        _current_position += n;
        return out;
    }

    [[gnu::noinline]] [[gnu::cold]] void skip_slow(size_t n) noexcept {
        auto left = std::min<size_t>(n, bytes_left());
        while (left) {
            auto this_length = std::min<size_t>(
              left, _current_end - _current_position);
            left -= this_length;
            if (left) {
                next_fragment();
            } else {
                _current_position += this_length;
            }
        }
    }

public:
    struct default_exception_thrower {
        [[noreturn]] [[gnu::cold]] static void
        throw_out_of_range(size_t attempted_read, size_t actual_left) {
            throw std::out_of_range(fmt::format(
              "attempted to read {:d} bytes from a {:d} byte buffer",
              attempted_read,
              actual_left));
        }
    };

    class iterator;

    CONCEPT(
      static_assert(fragmented_temporary_buffer_concepts::ExceptionThrower<
                    default_exception_thrower>);)

    istream(const vector_type& fragments, size_t total_size) noexcept
      : _current(fragments.begin())
      , _current_position(total_size ? _current->get() : nullptr)
      , _current_end(total_size ? _current->get() + _current->size() : nullptr)
      , _bytes_left(total_size) {
    }

    size_t bytes_left() const noexcept {
        return _bytes_left ? _bytes_left - (_current_position - _current->get())
                           : 0;
    }

    void skip(size_t n) noexcept {
        auto new_end = _current_position + n;
        if (__builtin_expect(new_end > _current_end, false)) {
            return skip_slow(n);
        }
        _current_position = new_end;
    }

    template<typename T, typename ExceptionThrower = default_exception_thrower>
    CONCEPT(requires fragmented_temporary_buffer_concepts::ExceptionThrower<
            ExceptionThrower>)
    T read(ExceptionThrower&& exceptions = default_exception_thrower()) {
        auto new_end = _current_position + sizeof(T);
        if (__builtin_expect(new_end > _current_end, false)) {
            return read_slow<T>(std::forward<ExceptionThrower>(exceptions));
        }
        T obj;
        std::copy_n(
          _current_position, sizeof(T), reinterpret_cast<char*>(&obj));
        _current_position = new_end;
        return obj;
    }

    template<
      typename Output,
      typename ExceptionThrower = default_exception_thrower>
    CONCEPT(requires fragmented_temporary_buffer_concepts::ExceptionThrower<
            ExceptionThrower>)
    Output read_to(
      size_t n,
      Output out,
      ExceptionThrower&& exceptions = default_exception_thrower()) {
        auto new_end = _current_position + n;
        if (__builtin_expect(new_end > _current_end, false)) {
            return read_to_slow(
              n,
              std::forward<Output>(out),
              std::forward<ExceptionThrower>(exceptions));
        }
        out = std::copy(_current_position, new_end, out);
        _current_position = new_end;
        return out;
    }

    template<typename ExceptionThrower = default_exception_thrower>
    CONCEPT(requires fragmented_temporary_buffer_concepts::ExceptionThrower<
            ExceptionThrower>)
    bytes_view read_bytes_view(
      size_t n,
      bytes_ostream& linearization_buffer,
      ExceptionThrower&& exceptions = default_exception_thrower()) {
        auto new_end = _current_position + n;
        if (__builtin_expect(new_end <= _current_end, true)) {
            auto v = bytes_view(
              reinterpret_cast<const bytes::value_type*>(_current_position), n);
            _current_position = new_end;
            return v;
        }
        check_out_of_range(exceptions, n);
        auto ptr = linearization_buffer.write_place_holder(n);
        read_to_slow(n, ptr, std::forward<ExceptionThrower>(exceptions));
        return bytes_view(reinterpret_cast<const bytes::value_type*>(ptr), n);
    }

    using const_iterator = iterator;
    // Non-consuming iterator, from this point forward.
    iterator begin() const noexcept;
    iterator end() const noexcept;

private:
    vector_type::const_iterator _current;
    const char* _current_position;
    const char* _current_end;
    size_t _bytes_left = 0;
};

inline fragmented_temporary_buffer::istream
fragmented_temporary_buffer::get_istream() const noexcept {
    return istream(_fragments, _size_bytes);
}

class fragmented_temporary_buffer::reader {
public:
    seastar::future<fragmented_temporary_buffer>
    read_exactly(seastar::input_stream<char>& in, size_t length) {
        _fragments = std::vector<seastar::temporary_buffer<char>>();
        _left = length;
        return seastar::repeat_until_value([this, length, &in] {
            using f_t_b_opt = std::optional<fragmented_temporary_buffer>;
            if (!_left) {
                return seastar::make_ready_future<f_t_b_opt>(
                  fragmented_temporary_buffer(std::move(_fragments), length));
            }
            return in.read_up_to(_left).then(
              [this](seastar::temporary_buffer<char> buf) {
                  if (buf.empty()) {
                      return f_t_b_opt(fragmented_temporary_buffer());
                  }
                  _left -= buf.size();
                  _fragments.emplace_back(std::move(buf));
                  return f_t_b_opt();
              });
        });
    }

private:
    std::vector<seastar::temporary_buffer<char>> _fragments;
    size_t _left = 0;
};

class fragmented_temporary_buffer::istream::iterator {
    void next_fragment() noexcept {
        _bytes_left -= _current_fragment->size();
        if (_bytes_left) {
            ++_current_fragment;
            _current_position = _current_fragment->begin();
        } else {
            _current_position = nullptr;
        }
    }

public:
    // iterator_traits
    using value_type = char;
    using pointer = const char*;
    using reference = const char&;
    using iterator_category = std::forward_iterator_tag;

    iterator(
      vector_type::const_iterator fragment,
      const char* current_position,
      size_t bytes_left) noexcept
      : _current_fragment(std::move(fragment))
      , _current_position(current_position)
      , _bytes_left(bytes_left) {
        if (_current_position == _current_fragment->end()) {
            next_fragment();
        }
    }

    struct end_tag {};
    iterator(end_tag) noexcept {
    }

    reference operator*() const noexcept {
        return *_current_position;
    }

    pointer operator->() const noexcept {
        return _current_position;
    }

    iterator& operator++() noexcept {
        if (++_current_position == _current_fragment->end()) {
            next_fragment();
        }
        return *this;
    }

    iterator operator++(int) noexcept {
        auto it = *this;
        operator++();
        return it;
    }

    bool operator==(const iterator& other) const noexcept {
        return _current_position == other._current_position;
    }

    bool operator!=(const iterator& other) const noexcept {
        return !(*this == other);
    }

private:
    vector_type::const_iterator _current_fragment;
    const char* _current_position = nullptr;
    size_t _bytes_left;
};

inline fragmented_temporary_buffer::istream::iterator
fragmented_temporary_buffer::istream::begin() const noexcept {
    return iterator(_current, _current_position, _bytes_left);
}

inline fragmented_temporary_buffer::istream::iterator
fragmented_temporary_buffer::istream::end() const noexcept {
    return iterator(iterator::end_tag{});
}
