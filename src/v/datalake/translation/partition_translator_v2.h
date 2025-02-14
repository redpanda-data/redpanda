/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "datalake/translation/deps.h"
#include "datalake/translation/scheduling.h"
#include "utils/prefix_logger.h"
#include "utils/retry_chain_node.h"

namespace datalake::translation {

class partition_translator : public scheduling::translator {
public:
    explicit partition_translator(
      ss::scheduling_group,
      std::unique_ptr<coordinator_api>,
      std::unique_ptr<data_source>,
      std::unique_ptr<translation_context>);

    const scheduling::translator_id& id() const override;

    ss::future<> init(
      scheduling::scheduling_notifications&,
      scheduling::reservations_tracker&) override;

    ss::future<> close() noexcept override;

    scheduling::translation_status status() const override;

    void start_translation(scheduling::clock::duration time_slice) override;

    void stop_translation() override;

    void reconcile_properties() noexcept override;

private:
    struct inflight_translation_state {
        scheduling::clock::duration translate_for;
        ss::abort_source as;
    };

    ss::future<> translate_until_stopped();

    ss::future<> translate_when_notified(kafka::offset begin_offset);

    ss::future<coordinator::fetch_latest_translated_offset_reply>
    fetch_latest_translated_offset(retry_chain_node&);

    ss::future<coordinator::add_translated_data_files_reply>
    checkpoint_translation_result(
      retry_chain_node&, coordinator::translated_offset_range);

    scheduling::clock::duration _target_lag;
    ss::scheduling_group _sg;
    std::unique_ptr<coordinator_api> _coordinator;
    std::unique_ptr<data_source> _data_source;
    std::unique_ptr<translation_context> _translation_ctx;
    model::term_id _term;
    prefix_logger _logger;
    bool _initialized = false;
    // set in init()
    scheduling::scheduling_notifications* _scheduler{nullptr};
    scheduling::reservations_tracker* _reservations{nullptr};
    ss::gate _gate;
    ss::abort_source _as;

    std::optional<inflight_translation_state> _inflight_translation_state;
    ss::condition_variable _ready_to_translate;
};
} // namespace datalake::translation
