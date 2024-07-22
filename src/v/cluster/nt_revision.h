/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "model/fundamental.h"
#include "model/metadata.h"
#include "serde/rw/envelope.h"

#include <absl/hash/hash.h>

namespace cluster {

/**
 * The namespace-topic-revision tuple refers to a particular incarnation
 * of a named topic.  For topic lifecycle markers,
 */
struct nt_revision
  : serde::envelope<nt_revision, serde::version<0>, serde::compat_version<0>> {
    model::topic_namespace nt;

    // The initial revision ID of partition 0.
    model::initial_revision_id initial_revision_id;

    template<typename H>
    friend H AbslHashValue(H h, const nt_revision& ntr) {
        return H::combine(std::move(h), ntr.nt, ntr.initial_revision_id);
    }

    friend bool operator==(const nt_revision& lhs, const nt_revision& rhs);
    friend std::ostream& operator<<(std::ostream&, const nt_revision&);

    auto serde_fields() { return std::tie(nt, initial_revision_id); }
};

struct nt_revision_hash {
    using is_transparent = void;

    size_t operator()(const nt_revision& v) const {
        return absl::Hash<nt_revision>{}(v);
    }
};

struct nt_revision_eq {
    using is_transparent = void;

    bool operator()(const nt_revision& lhs, const nt_revision& rhs) const {
        return lhs.nt == rhs.nt
               && lhs.initial_revision_id == rhs.initial_revision_id;
    }
};

} // namespace cluster
