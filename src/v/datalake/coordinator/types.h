/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "container/fragmented_vector.h"
#include "datalake/coordinator/translated_offset_range.h"
#include "datalake/errors.h"
#include "model/fundamental.h"
#include "serde/rw/enum.h"
#include "serde/rw/envelope.h"

namespace datalake::coordinator {

struct translated_data_file_entry
  : serde::envelope<
      translated_data_file_entry,
      serde::version<0>,
      serde::compat_version<0>> {
    model::topic_partition tp;

    // Translated data files, expected to be contiguous, with no gaps or
    // overlaps, ordered in increasing offset order.
    chunked_vector<translated_offset_range> translated_ranges;

    auto serde_fields() { return std::tie(tp, translated_ranges); }
};

struct add_translated_data_files_reply
  : serde::envelope<
      add_translated_data_files_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    add_translated_data_files_reply() = default;
    explicit add_translated_data_files_reply(coordinator_errc err)
      : errc(err) {}

    coordinator_errc errc;

    auto serde_fields() { return std::tie(errc); }
};
struct add_translated_data_files_request
  : serde::envelope<
      add_translated_data_files_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    using resp_t = add_translated_data_files_reply;

    add_translated_data_files_request() = default;

    model::topic_partition tp;
    chunked_vector<translated_data_file_entry> files;
    model::term_id translator_term;

    const model::topic_partition& topic_partition() const { return tp; }

    auto serde_fields() { return std::tie(tp, files, translator_term); }
};

struct fetch_latest_data_file_reply
  : serde::envelope<
      fetch_latest_data_file_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    fetch_latest_data_file_reply() = default;
    explicit fetch_latest_data_file_reply(coordinator_errc err)
      : errc(err) {}
    explicit fetch_latest_data_file_reply(std::optional<kafka::offset> o)
      : last_added_offset(o)
      , errc(coordinator_errc::ok) {}

    // The offset of the latest data file added to the coordinator.
    std::optional<kafka::offset> last_added_offset;

    // If not ok, the request processing has a problem.
    coordinator_errc errc;

    auto serde_fields() { return std::tie(last_added_offset, errc); }
};

struct fetch_latest_data_file_request
  : serde::envelope<
      fetch_latest_data_file_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    using resp_t = fetch_latest_data_file_reply;

    fetch_latest_data_file_request() = default;

    model::topic_partition tp;

    const model::topic_partition& topic_partition() const { return tp; }

    auto serde_fields() { return std::tie(tp); }
};

} // namespace datalake::coordinator
