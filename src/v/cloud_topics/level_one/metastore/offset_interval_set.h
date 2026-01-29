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

namespace cloud_topics::l1 {

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
    template<bool reverse = false>
    class stream {
    public:
        using iterator_t = std::conditional_t<
          reverse,
          typename iset_t::const_reverse_iterator,
          typename iset_t::const_iterator>;

        explicit stream(const iset_t& underlying)
          : set_(underlying) {
            if constexpr (reverse) {
                iter_ = set_.rbegin();
                end_ = set_.rend();
            } else {
                iter_ = set_.begin();
                end_ = set_.end();
            }
        }
        bool has_next() const noexcept { return iter_ != end_; }
        interval next() {
            vassert(has_next(), "next() called while has_next() is false");
            interval ret{
              .base_offset = kafka::offset(iter_->first),
              .last_offset = kafka::offset(iter_->second - 1),
            };
            ++iter_;
            return ret;
        }

    private:
        const iset_t& set_;
        iterator_t iter_;
        iterator_t end_;
    };

    bool empty() const;
    bool insert(kafka::offset base, kafka::offset last);
    bool contains(kafka::offset offset) const;
    bool covers(kafka::offset start, kafka::offset end) const;
    stream<false> make_stream() const { return stream(iset_); }
    stream<true> make_reverse_stream() const { return stream<true>(iset_); }
    chunked_vector<interval> to_vec() const;
    void truncate_with_new_start_offset(kafka::offset);

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "{}", iset_);
    }

private:
    interval_set<kafka::offset::type> iset_;
};

} // namespace cloud_topics::l1

template<>
struct fmt::formatter<cloud_topics::l1::offset_interval_set::interval> final
  : fmt::formatter<std::string_view> {
    using iset = cloud_topics::l1::offset_interval_set;
    template<typename FormatContext>
    auto format(const iset::interval& iv, FormatContext& ctx) const {
        return fmt::format_to(
          ctx.out(), "[{}, {}]", iv.base_offset, iv.last_offset);
    }
};
