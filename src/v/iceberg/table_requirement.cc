/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/table_requirement.h"

#include "base/vlog.h"
#include "iceberg/table_metadata.h"
#include "utils/to_string.h"

namespace iceberg::table_requirement {

namespace {

struct check_visitor {
    explicit check_visitor(const table_metadata* opt_metadata)
      : opt_metadata(opt_metadata) {}

    const table_metadata* opt_metadata;

    checked<std::nullopt_t, ss::sstring> operator()(const assert_create&) {
        if (opt_metadata) {
            return ssx::sformat(
              "table already exists (uuid: {})", opt_metadata->table_uuid);
        }
        return std::nullopt;
    }

    checked<std::nullopt_t, ss::sstring>
    operator()(const assert_table_uuid& req) {
        if (!opt_metadata) {
            return fmt_with_ctx(ssx::sformat, "table doesn't exist");
        }
        if (opt_metadata->table_uuid != req.uuid) {
            return ssx::sformat(
              "table uuid mismatch, expected {}, got {}",
              req.uuid,
              opt_metadata->table_uuid);
        }
        return std::nullopt;
    }

    checked<std::nullopt_t, ss::sstring>
    operator()(const last_assigned_field_match& req) {
        if (!opt_metadata) {
            return fmt_with_ctx(ssx::sformat, "table doesn't exist");
        }
        if (opt_metadata->last_column_id != req.last_assigned_field_id) {
            return ssx::sformat(
              "last assigned field id mismatch, expected {}, got {}",
              req.last_assigned_field_id,
              opt_metadata->last_column_id);
        }
        return std::nullopt;
    }

    checked<std::nullopt_t, ss::sstring>
    operator()(const assert_current_schema_id& req) {
        if (!opt_metadata) {
            return fmt_with_ctx(ssx::sformat, "table doesn't exist");
        }
        if (opt_metadata->current_schema_id != req.current_schema_id) {
            return ssx::sformat(
              "current schema id mismatch, expected {}, got {}",
              req.current_schema_id,
              opt_metadata->current_schema_id);
        }
        return std::nullopt;
    }

    checked<std::nullopt_t, ss::sstring>
    operator()(const assert_last_assigned_partition_id& req) {
        if (!opt_metadata) {
            return fmt_with_ctx(ssx::sformat, "table doesn't exist");
        }
        if (opt_metadata->last_partition_id != req.last_assigned_partition_id) {
            return ssx::sformat(
              "last partition id mismatch, expected {}, got {}",
              req.last_assigned_partition_id,
              opt_metadata->last_partition_id);
        }
        return std::nullopt;
    }

    checked<std::nullopt_t, ss::sstring>
    operator()(const assert_default_spec_id& req) {
        if (!opt_metadata) {
            return fmt_with_ctx(ssx::sformat, "table doesn't exist");
        }
        if (opt_metadata->default_spec_id != req.default_spec_id) {
            return ssx::sformat(
              "default partition spec id mismatch, expected {}, got {}",
              req.default_spec_id,
              opt_metadata->default_spec_id);
        }
        return std::nullopt;
    }

    checked<std::nullopt_t, ss::sstring>
    operator()(const assert_ref_snapshot_id& req) {
        if (!opt_metadata) {
            return fmt_with_ctx(ssx::sformat, "table doesn't exist");
        }

        std::optional<snapshot_id> cur_snapshot_id;
        if (req.ref == "main") {
            cur_snapshot_id = opt_metadata->current_snapshot_id;
        } else if (opt_metadata->refs) {
            auto it = opt_metadata->refs.value().find(req.ref);
            if (it != opt_metadata->refs.value().end()) {
                cur_snapshot_id = it->second.snapshot_id;
            }
        }

        bool matched = false;
        if (req.snapshot_id) {
            matched = req.snapshot_id.value() == cur_snapshot_id.value();
        } else {
            matched = !cur_snapshot_id;
        }
        if (!matched) {
            return ssx::sformat(
              "current snapshot id for ref {} mismatch, expected {}, got {}",
              req.ref,
              req.snapshot_id,
              cur_snapshot_id);
        }
        return std::nullopt;
    }
};

} // namespace

checked<std::nullopt_t, ss::sstring>
check(const requirement& req, const table_metadata* opt_metadata) {
    return std::visit(check_visitor{opt_metadata}, req);
}

} // namespace iceberg::table_requirement
