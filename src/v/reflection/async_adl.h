#pragma once
#include "reflection/adl.h"

#include <boost/range/irange.hpp>
namespace reflection {
template<typename T>
struct async_adl {
    using type = std::remove_reference_t<std::decay_t<T>>;
    // provide extension point for rpc on the cases that we need to
    ss::future<> to(iobuf& out, type t) {
        reflection::adl<type>{}.to(out, std::move(t));
        return ss::make_ready_future<>();
    }
    ss::future<type> from(iobuf_parser& p) {
        return ss::make_ready_future<type>(reflection::adl<type>{}.from(p));
    }
};

template<typename T>
struct async_adl<std::vector<T>> {
    using value_type = std::remove_reference_t<std::decay_t<T>>;

    ss::future<> to(iobuf& out, std::vector<value_type> t) {
        reflection::serialize<int32_t>(out, t.size());
        return ss::do_with(std::move(t), [&out](auto& t) {
            return ss::do_for_each(t, [&out](value_type& element) {
                return async_adl<value_type>{}.to(out, std::move(element));
            });
        });
    }

    ss::future<std::vector<value_type>> from(iobuf_parser& in) {
        const auto size = adl<int32_t>{}.from(in);
        std::vector<value_type> result;
        result.reserve(size);
        return ss::do_with(std::move(result), [size, &in](auto& result) {
            const auto r = boost::irange<size_t>(0, size);
            return ss::do_for_each(
                     r,
                     [&result, &in](size_t) {
                         return async_adl<value_type>{}.from(in).then(
                           [&result](auto d) {
                               result.push_back(std::move(d));
                           });
                     })
              .then([&result] { return std::move(result); });
        });
    }
};
} // namespace reflection
