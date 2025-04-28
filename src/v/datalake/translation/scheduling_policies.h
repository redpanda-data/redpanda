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

#include "config/property.h"
#include "datalake/translation/scheduling.h"

#include <absl/container/flat_hash_map.h>

namespace datalake::translation::scheduling {

/**
 * A very basic scheduling policy that schedules on a fcfs basis, used for
 * testing.
 */
class simple_fcfs_scheduling_policy : public scheduling_policy {
public:
    explicit simple_fcfs_scheduling_policy(
      config::binding<size_t> max_concurrent_translators,
      clock::duration translation_time_quota);

    ss::future<> schedule_one_translation(
      executor& executor, const reservations_tracker& mem_tracker) override;

    ss::future<> on_resource_exhaustion(
      executor& executor, const reservations_tracker& mem_tracker) override;

private:
    config::binding<size_t> _max_concurrent_translations;
    clock::duration _translation_time_quota;
};

/**
 * A scheduling policy that strives to meet the target lag deadline for
 * the translators while still being fair so that translators
 * with a small lag do not starve translators with large lag.
 *
 * The policy uses a heuristic with shares to guarantee a degree of
 * fairness proportional to the alloted shares.
 */
class fair_scheduling_policy : public scheduling_policy {
public:
    explicit fair_scheduling_policy(
      config::binding<size_t> max_concurrent_translators,
      clock::duration translation_time_quota);

    ss::future<>
    schedule_one_translation(executor&, const reservations_tracker&) override;

    ss::future<>
    on_resource_exhaustion(executor&, const reservations_tracker&) override;

private:
    // Minimum expected time slice allotment share of the total target lag.
    // For example, if target lag = 1hr, 0.3 * 1hr = 0.3h of translation time
    // is guaranteed to each translator over the period of lag interval.
    static constexpr double minimum_allotment_coeff = 0.3;

    // If the remaining time in the lag window falls within this fraction of
    // of the total lag time, it is considered as 'about to expire'.
    static constexpr double about_to_expire_window = 0.2;

    // A classification of waiting translators for prioritization.
    // Each of these groups is assigned shares that determine the likelihood
    // with which a translator from one of these groups is picked to run next.
    // During a translation tick, every waiting translator is classified into
    // one of these groups.

    // The rationale here is that we cannot always prioritize translators with
    // impending deadline as that will starve translators with larger lag
    // windows.
    enum translator_group : uint8_t {
        // A translator whose lag deadline has expired while it is waiting
        expired,
        // A translator whose lag deadline is about to expire soon.
        // Check {@link about_to_expire_window}.
        about_to_expire,
        // A translator that does not fall into the categories above
        // but has not met the minimum allotment quota.
        // Check {@link minimum_allotment_coeff}
        unfulfilled_quota,
        // Cannot be classified into one of the following groups
        other,
    };

    friend std::ostream& operator<<(std::ostream&, translator_group);

    /**
     * Picks a random translator group category with probability proportional
     * to the shares defined below.
     */
    translator_group choose_random_translator_group() const;

    // Higher the shares, higher the likelihood of the group being picked.
    // eg: other group with 5 shares results in a (5 / (5 + 15 + 30 + 50))
    // 5% chance of translator from that getting picked to run next.

    // todo: these weights can be made dynamic or mapped to a dynamic runtime
    // configuration.
    static constexpr long default_other_shares = 5;
    static constexpr long default_unfulfilled_group_shares = 15;
    static constexpr long default_about_to_expire_group_shares = 30;
    static constexpr long default_expired_group_shares = 50;

    using group_shares_t = absl::flat_hash_map<translator_group, long>;
    using group_intervals_t
      = absl::flat_hash_map<translator_group, std::pair<double, double>>;

    group_shares_t _group_to_shares;
    group_intervals_t _group_intervals;
    void initialize_group_shares();

    config::binding<size_t> _max_concurrent_translations;
    clock::duration _translation_time_quota;

    /*
     * state maintained for tracking the choice of which translator to finish
     * when servicing requests for immediate translator finish.
     *
     * id: translator id
     * status: status of the chosen translator
     */
    struct finish_choice_info {
        enum class status {
            running,
            waiting,
            idle,
        };

        std::string_view status_name() const {
            switch (status) {
            case status::running:
                return "running";
            case status::waiting:
                return "waiting";
            case status::idle:
                return "idle";
            }
        }

        // return the status of the translator
        static status translator_status(const translator_executable&);

        translator_id id;
        status status;
        translator::stop_request stop_request;

        finish_choice_info(
          translator_id id,
          enum status status,
          translator::stop_request request)
          : id(std::move(id))
          , status(status)
          , stop_request(request) {}
    };

    /*
     * select a translator from the set of translators requested for immediate
     * finish, if any. if a selection is made, it is removed from the
     * translators_for_immediate_finish index.
     */
    std::optional<finish_choice_info> choose_translator_to_finish(executor&);

    /*
     * process a translator choice to finish immediately
     */
    ss::future<> finish_translator(executor&, finish_choice_info);
};

} // namespace datalake::translation::scheduling
