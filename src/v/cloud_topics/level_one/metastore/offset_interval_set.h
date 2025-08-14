/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "container/chunked_vector.h"
#include "container/interval_set.h"
#include "model/fundamental.h"
#include "serde/envelope.h"
#include "serde/rw/envelope.h"
#include "serde/rw/map.h"

namespace experimental::cloud_topics::l1 {

// Wrapper around an interval_set, but with an interface that makes it
// condusive to using inclusive offset ranges.
class offset_interval_set
  : public serde::envelope<
      offset_interval_set,
      serde::version<0>,
      serde::compat_version<0>> {
public:
    using iset_t = interval_set<kafka::offset::type>;
    bool operator==(const offset_interval_set&) const = default;
    auto serde_fields() { return std::tie(iset_); }
    struct interval {
        kafka::offset base_offset;
        kafka::offset last_offset;
        friend std::ostream& operator<<(std::ostream&, const interval&);
    };
    class stream {
    public:
        explicit stream(const iset_t& underlying)
          : set_(underlying)
          , iter_(underlying.begin()) {}
        bool has_next() const noexcept;
        interval next();

    private:
        const iset_t& set_;
        iset_t::const_iterator iter_;
    };

    bool empty() const;
    bool insert(kafka::offset base, kafka::offset last);
    bool contains(kafka::offset offset) const;
    stream make_stream() const;
    chunked_vector<interval> to_vec() const;

private:
    interval_set<kafka::offset::type> iset_;
};

} // namespace experimental::cloud_topics::l1

template<>
struct fmt::formatter<
  experimental::cloud_topics::l1::offset_interval_set::interval>
  final : fmt::formatter<std::string_view> {
    using iset = experimental::cloud_topics::l1::offset_interval_set;
    template<typename FormatContext>
    auto format(const iset::interval& iv, FormatContext& ctx) const {
        return fmt::format_to(
          ctx.out(), "[{}, {}]", iv.base_offset, iv.last_offset);
    }
};
