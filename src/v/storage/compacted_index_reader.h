#pragma once

#include "model/timeout_clock.h"
#include "storage/compacted_index.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/file.hh>

#include <memory>
namespace storage {

// clang-format off
CONCEPT(template<typename Consumer> concept bool CompactedIndexEntryConsumer() {
    return requires(Consumer c, compacted_index::entry&& b) {
        { c(std::move(b)) } -> ss::future<ss::stop_iteration>;
        c.end_of_stream();
    };
})
// clang-format on

/// use this like a shared-pointer and pass around a copy
class compacted_index_reader {
public:
    class impl {
    public:
        explicit impl(ss::sstring filename) noexcept
          : _name(std::move(filename)) {}
        virtual ~impl() noexcept = default;
        impl(impl&&) noexcept = default;
        impl& operator=(impl&&) noexcept = default;
        impl(const impl&) = delete;
        impl& operator=(const impl&) = delete;

        virtual ss::future<> verify_integrity() = 0;

        virtual ss::future<> close() = 0;

        virtual ss::future<compacted_index::footer> load_footer() = 0;

        virtual void reset() = 0;

        virtual void print(std::ostream&) const = 0;

        const ss::sstring& filename() const { return _name; }

        virtual bool is_end_of_stream() const = 0;

        virtual ss::future<ss::circular_buffer<compacted_index::entry>>
          load_slice(model::timeout_clock::time_point) = 0;

        template<typename Consumer>
        auto
        consume(Consumer consumer, model::timeout_clock::time_point timeout) {
            return ss::do_with(
              std::move(consumer), [this, timeout](Consumer& consumer) {
                  return do_consume(consumer, timeout);
              });
        }

    private:
        compacted_index::entry pop_batch() {
            compacted_index::entry batch = std::move(_slice.front());
            _slice.pop_front();
            return batch;
        }

        bool is_slice_empty() const { return _slice.empty(); }

        ss::future<> do_load_slice(model::timeout_clock::time_point t) {
            return load_slice(t).then(
              [this](ss::circular_buffer<compacted_index::entry> next) {
                  _slice = std::move(next);
              });
        }

        template<typename Consumer>
        auto do_consume(
          Consumer& consumer, model::timeout_clock::time_point timeout) {
            return ss::repeat([this, timeout, &consumer] {
                       if (likely(!is_slice_empty())) {
                           return consumer(pop_batch());
                       }
                       if (is_end_of_stream()) {
                           return ss::make_ready_future<ss::stop_iteration>(
                             ss::stop_iteration::yes);
                       }
                       return do_load_slice(timeout).then(
                         [] { return ss::stop_iteration::no; });
                   })
              .then([&consumer] { return consumer.end_of_stream(); });
        }

        ss::sstring _name;
        ss::circular_buffer<compacted_index::entry> _slice;
    };

    explicit compacted_index_reader(ss::shared_ptr<impl> i) noexcept
      : _impl(std::move(i)) {}

    ss::future<> close() { return _impl->close(); }

    ss::future<> verify_integrity() { return _impl->verify_integrity(); }

    ss::future<compacted_index::footer> load_footer() {
        return _impl->load_footer();
    }

    void print(std::ostream& o) const { _impl->print(o); }

    void reset() { _impl->reset(); }

    const ss::sstring& filename() const { return _impl->filename(); }

    template<typename Consumer>
    CONCEPT(requires CompactedIndexEntryConsumer<Consumer>())
    auto consume(Consumer consumer, model::timeout_clock::time_point timeout) {
        return _impl->consume(std::move(consumer), timeout);
    }

private:
    ss::shared_ptr<impl> _impl;
};

compacted_index_reader make_file_backed_compacted_reader(
  ss::sstring filename, ss::file, ss::io_priority_class, size_t step_chunk);

inline ss::future<ss::circular_buffer<compacted_index::entry>>
compaction_index_reader_to_memory(compacted_index_reader rdr) {
    struct consumer {
        ss::future<ss::stop_iteration> operator()(compacted_index::entry b) {
            data.push_back(std::move(b));
            return ss::make_ready_future<ss::stop_iteration>(
              ss::stop_iteration::no);
        }
        ss::circular_buffer<compacted_index::entry> end_of_stream() {
            return std::move(data);
        };
        ss::circular_buffer<compacted_index::entry> data;
    };
    rdr.reset();
    return rdr.load_footer().discard_result().then([rdr]() mutable {
        return rdr.consume(consumer{}, model::no_timeout).finally([rdr] {});
    });
}
inline std::ostream&
operator<<(std::ostream& o, const compacted_index_reader& r) {
    r.print(o);
    return o;
}
} // namespace storage
