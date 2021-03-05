/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "reflection/adl.h"
#include "ssx/future-util.h"

#include <boost/range/irange.hpp>

#include <optional>
#include <vector>

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

/// Specializations of async_adl for nested types, optional and vector
template<typename T>
struct async_adl<std::optional<T>> {
    using value_type = std::remove_reference_t<std::decay_t<T>>;

    ss::future<> to(iobuf& out, std::optional<value_type> t) {
        if (t) {
            adl<int8_t>{}.to(out, 1);
            return async_adl<value_type>{}.to(out, std::move(t.value()));
        }
        adl<int8_t>{}.to(out, 0);
        return ss::now();
    }

    ss::future<std::optional<value_type>> from(iobuf_parser& in) {
        int8_t is_set = adl<int8_t>{}.from(in);
        if (is_set == 0) {
            return ss::make_ready_future<std::optional<value_type>>(
              std::nullopt);
        }
        return async_adl<value_type>{}.from(in).then([](value_type vt) {
            return ss::make_ready_future<std::optional<value_type>>(
              std::move(vt));
        });
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
        return ss::do_with(
          boost::irange<size_t>(0, size),
          [&in](const boost::integer_range<size_t>& r) {
              return ssx::async_transform(r.begin(), r.end(), [&in](size_t) {
                  return async_adl<value_type>{}.from(in);
              });
          });
    }
};
} // namespace reflection
