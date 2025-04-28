/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/translation/scheduling_policies.h"

#include "config/configuration.h"
#include "datalake/logger.h"
#include "random/generators.h"

#include <seastar/core/sleep.hh>

#include <queue>

using namespace std::chrono_literals;

static constexpr auto polling_interval = 1s;

namespace datalake::translation::scheduling {
simple_fcfs_scheduling_policy::simple_fcfs_scheduling_policy(
  config::binding<size_t> max_concurrent_translators,
  clock::duration translation_time_quota)
  : _max_concurrent_translations(std::move(max_concurrent_translators))
  , _translation_time_quota(translation_time_quota) {
    vlog(
      datalake_log.info,
      "created simple_fcfs_scheduling_policy policy with {} translators "
      "and {} time quota",
      max_concurrent_translators(),
      std::chrono::duration_cast<std::chrono::milliseconds>(
        translation_time_quota));
}

ss::future<> simple_fcfs_scheduling_policy::schedule_one_translation(
  executor& executor, const reservations_tracker& mem_tracker) {
    // check the # of running translators
    while (!executor.as.abort_requested() && !executor.waiting.empty()
           && !mem_tracker.memory_exhausted()
           && executor.running.size() >= _max_concurrent_translations()) {
        co_await ss::sleep_abortable(polling_interval, executor.as);
    }
    if (executor.as.abort_requested() || mem_tracker.memory_exhausted()) {
        co_return;
    }
    // pick the first queued translator.
    if (executor.waiting.empty()) {
        co_return;
    }
    executor.start_translation(
      *executor.waiting.begin(), _translation_time_quota);
}

ss::future<> simple_fcfs_scheduling_policy::on_resource_exhaustion(
  executor& executor, const reservations_tracker& mem_tracker) {
    while (mem_tracker.memory_exhausted() && !executor.as.abort_requested()) {
        // pick the earliest scheduled translator and force a flush.
        auto request = translator::stop_request{
          .reason = translator::stop_reason::oom, .cleanup_staged_data = true};
        if (!executor.running.empty()) {
            executor.stop_translation(*executor.running.begin(), request);
        }
        co_await ss::sleep_abortable(5s, executor.as);
    }
}

fair_scheduling_policy::fair_scheduling_policy(
  config::binding<size_t> max_concurrent_translators,
  clock::duration translation_time_quota)
  : _max_concurrent_translations(std::move(max_concurrent_translators))
  , _translation_time_quota(translation_time_quota) {
    vlog(
      datalake_log.info,
      "created fair_scheduling_policy policy with {} translators "
      "and {} time quota",
      max_concurrent_translators(),
      std::chrono::duration_cast<std::chrono::milliseconds>(
        translation_time_quota));
    initialize_group_shares();
}

std::ostream&
operator<<(std::ostream& os, fair_scheduling_policy::translator_group group) {
    switch (group) {
    case fair_scheduling_policy::translator_group::other:
        return os << "translator_group::other";
    case fair_scheduling_policy::translator_group::unfulfilled_quota:
        return os << "translator_group::unfulfilled_quota";
    case fair_scheduling_policy::translator_group::about_to_expire:
        return os << "translator_group::about_to_expire";
    case fair_scheduling_policy::translator_group::expired:
        return os << "translator_group::expired";
    }
}

void fair_scheduling_policy::initialize_group_shares() {
    _group_to_shares = {
      {other, fair_scheduling_policy::default_other_shares},
      {unfulfilled_quota,
       fair_scheduling_policy::default_unfulfilled_group_shares},
      {about_to_expire,
       fair_scheduling_policy::default_about_to_expire_group_shares},
      {expired, fair_scheduling_policy::default_expired_group_shares}};

    const double total_shares = std::accumulate(
      std::begin(_group_to_shares),
      std::end(_group_to_shares),
      0,
      [](const double so_far, const auto& p) { return so_far + p.second; });

    vassert(
      total_shares > 0,
      "invalid share assignment for translation groups, total shares should be "
      "> 0");

    double previous_end = 0.0;
    for (const auto& [group, shares] : _group_to_shares) {
        double end = previous_end + shares / total_shares;
        _group_intervals[group] = std::make_pair(previous_end, end);
        previous_end = end;
    }
}

fair_scheduling_policy::translator_group
fair_scheduling_policy::choose_random_translator_group() const {
    auto random = random_generators::get_real<double>(0, 1);
    auto it = std::ranges::find_if(
      _group_intervals, [&random](const auto& entry) {
          auto interval_begin = entry.second.first;
          auto interval_end = entry.second.second;
          return interval_begin <= random && random <= interval_end;
      });
    vassert(
      it != _group_intervals.end(),
      "Invalid share assignment for translation groups, no group found for {}",
      random);
    return it->first;
}

ss::future<> fair_scheduling_policy::schedule_one_translation(
  executor& executor, const reservations_tracker& mem_tracker) {
    // Wait until an empty slot frees up or control should yield back to the
    // scheduling loop because, for example, resource exhaustion needs to be
    // attended to.
    //
    // TODO: note that the polling interval here is 1s which is quite
    // responsive. However, if it longer then one should consider not using a
    // sleep here because it would degrade responsiveness to _state_changed_cvar
    // signals.
    while (!executor.as.abort_requested() && !executor.waiting.empty()
           && !mem_tracker.memory_exhausted()
           && executor.translators_for_immediate_finish.empty()
           && executor.running.size() >= _max_concurrent_translations()) {
        co_await ss::sleep_abortable(polling_interval, executor.as);
    }

    if (
      executor.as.abort_requested() || mem_tracker.memory_exhausted()
      || !executor.translators_for_immediate_finish.empty()) {
        co_return;
    }

    auto prioritized_group = choose_random_translator_group();
    vlog(
      datalake_log.trace,
      "prioritizing translator group of type: {}",
      prioritized_group);

    struct entry {
        translator_id id;
        translator_group group;
        // weight within the group, used to sort entries within
        // a single group.
        long weight{0};
    };
    // A comparator to sort all the {@link entry} entries based on two
    // dimensions
    // 1. group - if the group is prioritized, it is bubbled to the top relative
    //  to other groups (since we want to pick the prioritized group among all
    //  the available translators)
    // 2. weight - 2nd dimension of sorting among all translators within a group

    // This scheme allows us to always pick a translator if a prioritized group
    // is unavailable. In such a case pick a group with highest shares (and
    // weight).
    auto max_heap_cmp = [&](const entry& lhs, const entry& rhs) {
        auto both_prioritized = lhs.group == prioritized_group
                                && rhs.group == prioritized_group;
        if (both_prioritized) {
            return lhs.weight < rhs.weight;
        }
        // Either one of them is prioritized
        if (lhs.group == prioritized_group || rhs.group == prioritized_group) {
            return rhs.group == prioritized_group;
        }
        // neither of them are prioritized, just sort based on
        // shares if they are different, else weight within the same share
        // group.
        auto lhs_shares = _group_to_shares[lhs.group];
        auto rhs_shares = _group_to_shares[rhs.group];
        return lhs_shares == rhs_shares ? lhs.weight < rhs.weight
                                        : lhs_shares < rhs_shares;
    };

    // Make a snapshot of state to be scheduled without any scheduling points.
    // The expectation is that the waiting queue is small enough to not
    // cause any reactor stalls.

    // This loop classifies each translator into a group {@link
    // translator_group} depending on the current status of the group. A
    // snapshot of all the classified translators is then sorted using the
    // comparator prioritizing the group that is randomly picked above..

    chunked_vector<entry> candidates;
    for (const auto& translator : executor.waiting) {
        auto status = translator.status();
        auto duration_to_expire = status.next_checkpoint_deadline
                                  - clock::now();
        auto& id = translator.translator_ptr()->id();
        if (duration_to_expire < clock::duration{0}) {
            // Translator with the largest expiration time (most expired) is
            // given the highest weight.
            candidates.push_back(
              {.id = id,
               .group = translator_group::expired,
               .weight = -duration_to_expire.count()});
        } else if (
          duration_to_expire < about_to_expire_window * status.target_lag) {
            // Translator with the nearest expiry time is prioritized higher.
            candidates.push_back(
              {.id = id,
               .group = translator_group::about_to_expire,
               .weight = -duration_to_expire.count()});
        } else {
            auto total_time = clock::now() - translator.start_time();
            auto total_running_time = translator.total_running_time();
            if (total_running_time < minimum_allotment_coeff * total_time) {
                // within all unfulfilled quota, we want to order order them
                // by least alloted time first .
                candidates.push_back(
                  {.id = id,
                   .group = translator_group::unfulfilled_quota,
                   .weight = -total_running_time.count()});
            } else {
                // todo: perhaps we can order by pending_bytes_to_translate
                candidates.push_back(
                  {.id = id,
                   .group = translator_group::other,
                   .weight = random_generators::get_int<long>()});
            }
        }
        const auto& back = candidates.back();
        vlog(
          datalake_log.trace,
          "scheduling candidate: {},  group: {}, weight: {}",
          translator,
          back.group,
          back.weight);
    }

    co_await ss::maybe_yield();

    // Reactor friendly sort using the comparator
    std::priority_queue<entry, chunked_vector<entry>, decltype(max_heap_cmp)>
      prioritized(max_heap_cmp);
    for (auto& entry : candidates) {
        executor.as.check();
        auto it = executor.translators.find(entry.id);
        if (
          it == executor.translators.end()
          || !it->second._waiting_hook.is_linked()) {
            continue;
        }
        prioritized.push(entry);
        co_await ss::maybe_yield();
    }

    executor.as.check();

    while (!prioritized.empty()) {
        auto& entry = prioritized.top();
        auto it = executor.translators.find(entry.id);
        if (
          it != executor.translators.end()
          && it->second._waiting_hook.is_linked()) {
            vlog(
              datalake_log.trace,
              "[{}] chosen translator to run, group: {}, weight: {}",
              it->second,
              entry.group,
              entry.weight);
            executor.start_translation(it->second, _translation_time_quota);
            co_return;
        }
        prioritized.pop();
        co_await ss::maybe_yield();
    }
}

/*
 * when resource exhaustion is reported we seek to stop and/or finish
 * translators in order to free up memory or disk usage.
 *
 * disk usage
 * ==========
 * this case is indicated below by a non-empty set of translators that have been
 * requested to immediately finish (see datalake_manager::disk_space_monitor).
 * in this case the translator with the most usage is selected to be finished
 * immediately so that it will upload and delete its on disk staging data. there
 * are three scenarios depending on the state of the translator:
 *
 * 1) running
 *
 * currently we prioritize translators in the running state (even over
 * those with larger usages that are non-running) under the assumption that they
 * are (a) in the requested set and (b) have the potential to make the situation
 * worse since they are actively translating data.
 *
 * there are arguments against this policy. for instance, finishing a running
 * translator may make the situation worse still because its in-memory state
 * will be flushed to disk as part of the finishing process.
 *
 * 2) waiting
 *
 * when the translator is in the waiting state we start the translator, and
 * arrange for it to immediately stop itself and finish (without building a
 * reader or poking the translator context).
 *
 * 3) neither running nor waiting
 *
 * translator::finish_translation is used for this request. in the partition
 * translator implementation the offset polling loop will test for this request
 * and exit its polling loop when requested to finish.
 *
 * memory usage
 * ============
 *
 * stop the running translator (if any exist) with the highest reported memory
 * usage.
 */
ss::future<> fair_scheduling_policy::on_resource_exhaustion(
  executor& executor, const reservations_tracker& mem_tracker) {
    /*
     * run parallel finish requests.
     */
    while (true) {
        const size_t max_parallel
          = config::shard_local_cfg()
              .datalake_scheduler_max_concurrent_translations();

        std::vector<finish_choice_info> choices;
        while (choices.size() < max_parallel) {
            auto choice = choose_translator_to_finish(executor);
            if (!choice) {
                break;
            }
            choices.push_back(choice.value());
        }

        if (choices.empty()) {
            // fall through to handle memory resources if necessary
            break;
        }

        co_await ss::max_concurrent_for_each(
          choices, max_parallel, [this, &executor](auto& choice) {
              return finish_translator(executor, choice);
          });
    }

    if (!mem_tracker.memory_exhausted() || executor.running.empty()) {
        co_return;
    }
    // stop the translator with highest memory usage first.
    // note: the size of this list is super small (low single
    // digits)
    executor.running.sort(
      [](const translator_executable& a, const translator_executable& b) {
          return a.status().memory_bytes_reserved
                 > b.status().memory_bytes_reserved;
      });

    // pick the earliest scheduled translator and force a flush.
    auto& executable = *executor.running.begin();
    vlog(
      datalake_log.debug,
      "[{}] stopping translator due to memory exhaustion",
      executable);
    auto oom_stop_request = translator::stop_request{
      .reason = translator::stop_reason::oom, .cleanup_staged_data = true};
    co_await finish_translator(
      executor,
      finish_choice_info(
        executable.translator_ptr()->id(),
        finish_choice_info::status::running,
        oom_stop_request));
}

enum fair_scheduling_policy::finish_choice_info::status
fair_scheduling_policy::finish_choice_info::translator_status(
  const translator_executable& exe) {
    if (exe._running_hook.is_linked()) {
        return finish_choice_info::status::running;
    } else if (exe._waiting_hook.is_linked()) {
        return finish_choice_info::status::waiting;
    }
    return finish_choice_info::status::idle;
}

std::optional<fair_scheduling_policy::finish_choice_info>
fair_scheduling_policy::choose_translator_to_finish(executor& executor) {
    auto& translators_to_finish = executor.translators_for_immediate_finish;
    if (translators_to_finish.empty()) {
        return std::nullopt;
    }

    // choice: the return value
    //
    // finish_key: index into `translators_to_finish` so that we can remove the
    // final choice. we can't hold on to an iterator because the set is modified
    // during iteration.
    struct choice_info {
        finish_choice_info info;
        size_t finish_key;
    };
    std::optional<choice_info> choice;

    // find highest priority running translator
    for (auto it = translators_to_finish.begin();
         it != translators_to_finish.end();) {
        // map translator_id -> translator_executable
        auto eit = executor.translators.find(it->second.id);
        if (eit == executor.translators.end()) {
            it = translators_to_finish.erase(it);
            continue;
        }

        // potential return value for choice
        const auto curr_info = finish_choice_info(
          eit->first,
          finish_choice_info::translator_status(eit->second),
          it->second.stop_request);

        // first choice is a running translator
        if (curr_info.status == finish_choice_info::status::running) {
            choice = choice_info{.info = curr_info, .finish_key = it->first};
            break;
        }

        if (!choice.has_value()) {
            choice = choice_info{.info = curr_info, .finish_key = it->first};
            // continue looking for a running translator
        }

        ++it;
    }

    if (choice.has_value()) {
        translators_to_finish.erase(choice->finish_key);
        return choice->info;
    }

    return std::nullopt;
}

ss::future<> fair_scheduling_policy::finish_translator(
  executor& executor, finish_choice_info choice) {
    // because the translator may have disappeared
    const auto eit = executor.translators.find(choice.id);
    if (eit == executor.translators.end()) {
        co_return;
    }

    auto& executable = eit->second;
    auto stop_initiated_ts = clock::now();

    /*
     * it is possible that the status of the choice changed due to fiber
     * scheduling. we aren't going to change our decision, but we do need to
     * make sure that we handle the choice correctly below.
     */
    choice.status = finish_choice_info::translator_status(executable);

    /*
     * the translator needs special handling to finish promptly depending on its
     * current state.
     */
    switch (choice.status) {
    case finish_choice_info::status::running:
        executor.stop_translation(executable, choice.stop_request);
        break;

    case finish_choice_info::status::waiting:
        // this sets up the translator state such that when it starts it
        // will immediately stop itself and finish. we piggy back on the
        // out-of-memory exception which has "immediate finish"
        // semantics rather than adding completely new states.
        executor.start_translation(executable, _translation_time_quota);
        executor.stop_translation(executable, choice.stop_request);
        break;

    case finish_choice_info::status::idle:
        // no handling needed here. the translator is in a polling loop and will
        // see that the finish bit is set.
        break;
    }

    vlog(
      datalake_log.debug,
      "Scheduler requesting {} translator {} to finish, request: {}",
      choice.status_name(),
      choice.id,
      choice.stop_request);

    /*
     * set_finish_translation is called on the choice, so wait for the
     * translator to finish and clear the bit. this should work for translators
     * in any state. see partition_translator::run_one_translation_iteration for
     * the raii object that we use to make sure this bit is cleared eventually.
     */
    while (!executor.as.abort_requested()) {
        // done if the translator disappeared
        auto it = executor.translators.find(choice.id);
        if (it == executor.translators.end()) {
            break;
        }

        // done if the translator cleared its finish bit
        if (
          it->second.translator_ptr()->status().last_finish_time
          > stop_initiated_ts) {
            break;
        }

        co_await ss::sleep_abortable(polling_interval, executor.as);
    }
}

} // namespace datalake::translation::scheduling
