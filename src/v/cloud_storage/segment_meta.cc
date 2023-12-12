#include "segment_meta.h"

#include <fmt/ranges.h>

std::ostream&
cloud_storage::operator<<(std::ostream& o, const segment_meta& s) {
    fmt::print(o, "{}", s);
    return o;
}

std::ostream&
cloud_storage::operator<<(std::ostream& o, const segment_name_format& r) {
    switch (r) {
    case segment_name_format::v1:
        o << "{v1}";
        break;
    case segment_name_format::v2:
        o << "{v2}";
        break;
    case segment_name_format::v3:
        o << "{v3}";
        break;
    }
    return o;
}

auto fmt::formatter<cloud_storage::segment_meta>::format(
  cloud_storage::segment_meta const& s, format_context& ctx) const
  -> decltype(ctx.out()) {
    if (presentation == 'u') {
        return fmt::format_to(
          ctx.out(),
          "{{is_compacted: {}, size_bytes: {}, base_offset: {}, "
          "committed_offset: "
          "{}, base_timestamp: {}, max_timestamp: {}, delta_offset: {}, "
          "ntp_revision: {}, archiver_term: {}, segment_term: {}, "
          "delta_offset_end: {}, sname_format: {}, metadata_size_hint: "
          "{}}}",
          s.is_compacted,
          s.size_bytes,
          s.base_offset,
          s.committed_offset,
          s.base_timestamp,
          s.max_timestamp,
          s.delta_offset,
          s.ntp_revision,
          s.archiver_term,
          s.segment_term,
          s.delta_offset_end,
          s.sname_format,
          s.metadata_size_hint);
    }

    if (presentation == 's') {
        return fmt::format_to(
          ctx.out(),
          "{{o={}-{} t={}-{}}}",
          s.base_offset,
          s.committed_offset,
          s.base_timestamp,
          s.max_timestamp);
    }

    using namespace cloud_storage;
    // format one line
    constexpr static auto unwrap = []<typename T>(T const& v) {
        if constexpr (std::is_same_v<T, model::timestamp>) {
            return v();
        } else {
            return v;
        }
    };
    return fmt::format_to(
      ctx.out(),
      "{}",
      std::apply(
        [&](auto&... v) { return std::tuple(unwrap(v)...); },
        reflection::to_tuple(s)));
}
