#pragma once

#include "seastarx.h"

#include <seastar/core/future.hh>

#include <iterator>

namespace detail {
template<typename Container, typename Iterator, typename AsyncAction>
inline ss::future<Container>
copy_range(Iterator begin, Iterator end, AsyncAction action, Container c) {
    auto i = std::inserter(c, c.end());
    while (begin != end) {
        auto f = ss::futurize_apply(action, *begin++);
        if (!f.available()) {
            return f.then([begin = std::move(begin),
                           end = std::move(end),
                           action = std::move(action),
                           c = std::move(c)](auto v) mutable {
                auto i = std::inserter(c, c.end());
                *i++ = std::move(v);
                return copy_range(
                  std::move(begin),
                  std::move(end),
                  std::move(action),
                  std::move(c));
            });
        }
        if (f.failed()) {
            return ss::make_exception_future<Container>(f.get_exception());
        }
        *i++ = f.get0();
        if (ss::need_preempt()) {
            return copy_range(
              std::move(begin),
              std::move(end),
              std::move(action),
              std::move(c));
        }
    }
    return ss::make_ready_future<Container>(std::move(c));
}
} // namespace detail

/// Transforms the input range, applying to each element
/// the specified asynchronous function, waiting for it
/// to complete before applying the function to the next
/// element in the input range.
///
/// \param begin an \c InputIterator designating the beginning of the range
/// \param end an \c InputIterator designating the end of the range
/// \param action a callable, taking a reference to objects from the range
///               as a parameter, and returning a \c ss::futureT> that contains
///               the result of transforming an input element.
/// \return a future resolving to the new container on success,
/// or the first failed future if \c action failed. Order of elements is
/// preserved where applicable (i.e., the target Container has a notion of
/// order).
template<typename Container, typename Iterator, typename AsyncAction>
GCC6_CONCEPT(requires requires(AsyncAction aa, Iterator it, Container c) {
    ss::futurize_apply(aa, *it++);
    requires ss::is_future<decltype(ss::futurize_apply(aa, *it))>::value;
    *std::inserter(c, c.end()) = ss::futurize_apply(aa, *it).get0();
})
inline ss::future<Container> copy_range(
  Iterator begin, Iterator end, AsyncAction action) {
    Container r;
    using itraits = std::iterator_traits<Iterator>;
    r.reserve(ss::internal::iterator_range_estimate_vector_capacity(
      begin, end, typename itraits::iterator_category()));
    return detail::copy_range(
      std::move(begin), std::move(end), std::move(action), std::move(r));
}

/// Transforms the input range, applying to each element
/// the specified asynchronous function, waiting for it
/// to complete before applying the function to the next
/// element in the input range.
///
/// \param range a \c Range object designating input values
/// \param action a callable, taking a reference to objects from the range
///               as a parameter, and returning a \c ss::futureT> that contains
///               the result of transforming an input element.
/// \return a future resolving to the new container on success,
/// or the first failed future if \c action failed. Order of elements is
/// preserved where applicable (i.e., the target Container has a notion of
/// order).
template<typename Container, typename Range, typename AsyncAction>
GCC6_CONCEPT(requires requires(AsyncAction aa, Range r, Container c) {
    ss::futurize_apply(aa, *r.begin());
    requires ss::is_future<decltype(ss::futurize_apply(aa, *r.begin()))>::value;
    *std::inserter(c, c.end()) = ss::futurize_apply(aa, *r.begin()).get0();
})
inline ss::future<Container> copy_range(Range& r, AsyncAction action) {
    return copy_range<Container>(std::begin(r), std::end(r), std::move(action));
}
