#pragma once

#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timestamp.h"
#include "serde/envelope.h"

#include <fmt/core.h>

namespace cloud_storage {

/// Version of the segment name format
enum class segment_name_format : int16_t {
    // Original metadata format, segment name has simple format (same as on
    // disk).
    v1 = 1,
    // Extended format, segment name is different from on-disk representation,
    // the committed offset and size are added to the name.
    v2 = 2,
    // Extra field which is used to track size of the tx-manifest is added.
    v3 = 3
};

std::ostream& operator<<(std::ostream& o, const segment_name_format& r);

struct segment_meta {
    using value_t = segment_meta;
    static constexpr serde::version_t redpanda_serde_version = 3;
    static constexpr serde::version_t redpanda_serde_compat_version = 0;

    bool is_compacted;
    size_t size_bytes;
    model::offset base_offset;
    model::offset committed_offset;
    model::timestamp base_timestamp;
    model::timestamp max_timestamp;
    model::offset_delta delta_offset;

    model::initial_revision_id ntp_revision;
    model::term_id archiver_term;
    /// Term of the segment (included in segment file name)
    model::term_id segment_term;
    /// Offset translation delta just past the end of this segment. This is
    /// useful in determining the next Kafka offset, e.g. when reporting the
    /// high watermark.
    model::offset_delta delta_offset_end;
    /// Segment name format specifier
    segment_name_format sname_format{segment_name_format::v1};
    /// Size of the metadata (optional)
    uint64_t metadata_size_hint{0};

    kafka::offset base_kafka_offset() const {
        // Manifests created with the old version of redpanda won't have the
        // delta_offset field. In this case the value will be initialized to
        // model::offset::min(). In this case offset translation couldn't be
        // performed.
        auto delta = delta_offset == model::offset_delta::min()
                       ? model::offset_delta(0)
                       : delta_offset;
        return base_offset - delta;
    }

    // NOTE: in older versions of Redpanda, delta_offset_end may have been
    // under-reported by 1 if the last segment contained no data batches,
    // meaning next_kafka_offset could over-report the next offset by 1.
    kafka::offset next_kafka_offset() const {
        // Manifests created with the old version of redpanda won't have the
        // delta_offset_end field. In this case offset translation couldn't be
        // performed.
        auto delta = delta_offset_end == model::offset_delta::min()
                       ? model::offset_delta(0)
                       : delta_offset_end;
        // NOTE: delta_offset_end is a misnomer and actually refers to the
        // offset delta of the next offset after this segment.
        return model::next_offset(committed_offset) - delta;
    }

    kafka::offset last_kafka_offset() const {
        return next_kafka_offset() - kafka::offset(1);
    }

    auto operator<=>(const segment_meta&) const = default;

    template<typename H>
    friend H AbslHashValue(H h, const segment_meta& meta) {
        return H::combine(
          std::move(h),
          meta.base_offset(),
          meta.committed_offset(),
          meta.delta_offset(),
          meta.delta_offset_end());
    }
};

std::ostream& operator<<(std::ostream& o, const segment_meta& r);

} // namespace cloud_storage
