/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/write_request_scheduler/write_request_scheduler.h"

#include "base/vassert.h"
#include "cloud_topics/level_zero/common/extent_meta.h"
#include "cloud_topics/level_zero/common/level_zero_probe.h"
#include "cloud_topics/level_zero/pipeline/base_pipeline.h"
#include "cloud_topics/level_zero/pipeline/write_pipeline.h"
#include "cloud_topics/level_zero/pipeline/write_request.h"
#include "cloud_topics/logger.h"
#include "config/configuration.h"
#include "random/simple_time_jitter.h"
#include "ssx/future-util.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <seastar/coroutine/as_future.hh>

#include <algorithm>
#include <chrono>
#include <cstddef>

using namespace std::chrono_literals;

namespace cloud_topics::l0 {

namespace {
static constexpr auto scheduler_long_sleep_interval = 20ms;
static constexpr auto scheduler_short_sleep_interval = 5ms;
static constexpr auto scheduler_sleep_jitter = 2ms;

// Static zero counter used when next stage is not registered yet
static std::atomic<size_t> zero_counter{0};

// Copy extents and share the payload to use on another shard
serialized_chunk shallow_copy(serialized_chunk& chunk) {
    serialized_chunk copy;
    copy.extents = chunk.extents.copy();
    copy.payload = chunk.payload.share(0, chunk.payload.size_bytes());
    return copy;
}
} // namespace

template<typename Clock>
size_t scheduler_context<Clock>::count_active_groups() const {
    if (shard_to_group.empty()) {
        return 0;
    }
    // Groups are monotonically increasing due to buddy algorithm allocation.
    // Count transitions to new groups by tracking the last seen group.
    size_t count = 1;
    group_id last_group = shard_to_group[0].load();
    for (size_t i = 1; i < shard_to_group.size(); i++) {
        auto current_group = shard_to_group[i].load();
        if (current_group != last_group) {
            count++;
            last_group = current_group;
        }
    }
    return count;
}

template<typename Clock>
size_t scheduler_context<Clock>::get_group_size(group_id gid) const {
    size_t count = 0;
    for (const auto& g : shard_to_group) {
        if (g.load() == gid) {
            count++;
        }
    }
    return count;
}

template<typename Clock>
size_t scheduler_context<Clock>::get_shard_index_in_group(
  ss::shard_id shard, group_id gid) const {
    size_t index = 0;
    for (size_t i = 0; i < static_cast<size_t>(shard); i++) {
        if (shard_to_group[i].load() == gid) {
            index++;
        }
    }
    return index;
}

template<typename Clock>
uint64_t scheduler_context<Clock>::get_group_bytes(group_id gid) const {
    uint64_t total_bytes = 0;
    for (size_t i = 0; i < shards.size(); i++) {
        if (shard_to_group[i].load() == gid) {
            total_bytes += shards[i].backlog_size.get().load();
        }
    }
    return total_bytes;
}

template<typename Clock>
uint64_t
scheduler_context<Clock>::get_group_next_stage_bytes(group_id gid) const {
    uint64_t total_bytes = 0;
    for (size_t i = 0; i < shards.size(); i++) {
        if (shard_to_group[i].load() == gid) {
            const auto& ref = shards[i].next_stage_backlog_size;
            total_bytes += ref.get().load();
        }
    }
    return total_bytes;
}

template<typename Clock>
bool scheduler_context<Clock>::try_split_group(group_id gid) {
    // Find all shards in this group
    std::vector<size_t> group_shards;
    for (size_t i = 0; i < shards.size(); i++) {
        if (shard_to_group[i].load() == gid) {
            group_shards.push_back(i);
        }
    }

    // Can't split a group with only one shard
    if (group_shards.size() <= 1) {
        return false;
    }

    // Split in half: first half stays, second half moves to new group
    size_t split_point = group_shards.size() / 2;

    // New group ID = shard ID of first shard in second half
    group_id new_gid = group_id(group_shards[split_point]);

    // Move second half to the new group
    for (size_t i = split_point; i < group_shards.size(); i++) {
        shard_to_group[group_shards[i]].store(new_gid);
    }

    // Reset the new group's state
    auto now = Clock::now();
    groups[static_cast<size_t>(new_gid)].ix.store(0);
    groups[static_cast<size_t>(new_gid)].last_upload_time.store(now);
    groups[static_cast<size_t>(new_gid)].last_modification_time.store(now);

    // Update the original group's modification time
    groups[static_cast<size_t>(gid)].last_modification_time.store(now);

    return true;
}

template<typename Clock>
std::optional<group_id>
scheduler_context<Clock>::find_buddy_group(group_id gid) const {
    // Find the last shard in this group
    size_t last_shard_in_group = 0;
    bool found = false;
    for (size_t i = 0; i < shards.size(); i++) {
        if (shard_to_group[i].load() == gid) {
            last_shard_in_group = i;
            found = true;
        }
    }
    if (!found) {
        return std::nullopt;
    }

    // The buddy group starts at the next shard
    size_t buddy_first_shard = last_shard_in_group + 1;
    if (buddy_first_shard >= shards.size()) {
        return std::nullopt;
    }

    // Return the group ID of that shard
    return shard_to_group[buddy_first_shard].load();
}

template<typename Clock>
bool scheduler_context<Clock>::try_merge_group(group_id gid) {
    auto buddy = find_buddy_group(gid);
    if (!buddy.has_value()) {
        return false;
    }

    group_id buddy_gid = buddy.value();

    // Can only merge if buddy is a different group
    if (buddy_gid == gid) {
        return false;
    }

    // Move all shards from buddy group to this group
    for (size_t i = 0; i < shards.size(); i++) {
        if (shard_to_group[i].load() == buddy_gid) {
            shard_to_group[i].store(gid);
        }
    }

    // Reset this group's round-robin counter and update modification time
    auto now = Clock::now();
    groups[static_cast<size_t>(gid)].ix.store(0);
    groups[static_cast<size_t>(gid)].last_modification_time.store(now);

    return true;
}

template<typename Clock>
schedule_result<Clock> scheduler_context<Clock>::try_schedule_upload(
  ss::shard_id shard,
  size_t max_buffer_size,
  std::chrono::milliseconds scheduling_interval,
  time_point now,
  write_request_scheduler_probe& probe) {
    // Get group info for this shard
    auto gid = shard_to_group[shard].load();
    auto& group = groups[static_cast<size_t>(gid)];

    // First check round-robin: is it this shard's turn within the group?
    auto grp_size = get_group_size(gid);
    auto current_ix = group.ix.load();
    auto shard_index_in_group = get_shard_index_in_group(shard, gid);

    if ((current_ix % grp_size) != shard_index_in_group) {
        // Not this shard's turn - wait for next cycle
        return {schedule_action::skip, std::nullopt, gid};
    }

    // Check if this shard should evaluate group splitting or merging.
    // Only the first shard in the group (shard_index_in_group == 0) checks.
    // Rate-limit split/merge decisions to avoid group churn - only allow
    // modifications if enough time has passed since the last split/merge.
    if (shard_index_in_group == 0) {
        auto last_mod_time = group.last_modification_time.load();
        auto cooldown = scheduling_interval * 2;
        bool can_modify = (now - last_mod_time) >= cooldown;

        if (can_modify) {
            auto next_stage_bytes = get_group_next_stage_bytes(gid);
            probe.set_next_stage_bytes(next_stage_bytes);
            // Calculate dynamic split threshold based on group size
            size_t group_split_threshold = 2 * max_buffer_size * grp_size;
            if (grp_size > 1 && next_stage_bytes > group_split_threshold) {
                // Split: next stage is overloaded, work more independently
                if (try_split_group(gid)) {
                    vlog(cd_log.trace, "Split group {}", gid);
                } else {
                    vlog(cd_log.debug, "Failed to split group {}", gid);
                }
            } else if (next_stage_bytes == 0) {
                // Merge: next stage is empty, can work together with buddy
                // But only if buddy's next stage is also empty
                auto buddy = find_buddy_group(gid);
                if (buddy.has_value()) {
                    auto buddy_next_stage_bytes = get_group_next_stage_bytes(
                      buddy.value());
                    if (buddy_next_stage_bytes == 0) {
                        if (try_merge_group(gid)) {
                            // After merge, re-evaluate group info for this
                            // shard (gid stays the same since we absorbed
                            // the buddy)
                            grp_size = get_group_size(gid);
                            vlog(
                              cd_log.trace,
                              "Merged group {}, group size: {}",
                              gid,
                              grp_size);
                        }
                    } else {
                        vlog(
                          cd_log.debug,
                          "Failed to merge group {}, group size: {}",
                          gid,
                          grp_size);
                    }
                }
            }
        }
    }

    // It's this shard's turn - check upload conditions based on group state
    auto group_bytes = get_group_bytes(gid);
    auto last_upload_time
      = groups[static_cast<size_t>(gid)].last_upload_time.load();
    auto deadline = last_upload_time + scheduling_interval;

    // Check if upload conditions are met (group size threshold or deadline)
    if (
      group_bytes == 0 || (group_bytes < max_buffer_size && deadline >= now)) {
        return {schedule_action::wait, std::nullopt, gid};
    }

    // Try to acquire group mutex (non-blocking)
    std::unique_lock<std::mutex> lock(
      groups[static_cast<size_t>(gid)].mutex, std::try_to_lock);
    if (!lock.owns_lock()) {
        // Some other shard in the group is busy
        return {schedule_action::wait, std::nullopt, gid};
    }

    // Re-check conditions after acquiring lock to handle races
    group_bytes = get_group_bytes(gid);
    deadline = groups[static_cast<size_t>(gid)].last_upload_time.load()
               + scheduling_interval;
    now = Clock::now();

    if (group_bytes < max_buffer_size && deadline >= now) {
        return {schedule_action::wait, std::nullopt, gid};
    }

    // Create schedule_request RAII object - it will increment ix when destroyed
    auto& ix = groups[static_cast<size_t>(gid)].ix;
    return {
      schedule_action::upload,
      schedule_request<Clock>(std::move(lock), ix),
      gid};
}

template<typename Clock>
void scheduler_context<Clock>::record_upload_time(
  group_id gid, time_point upload_time) {
    groups[static_cast<size_t>(gid)].last_upload_time.store(upload_time);
}

template<typename Clock>
write_request_scheduler<Clock>::write_request_scheduler(
  write_pipeline<Clock>::stage s)
  : _stage(s)
  , _max_buffer_size(
      config::shard_local_cfg()
        .cloud_topics_produce_batching_size_threshold.bind())
  , _max_cardinality(
      config::shard_local_cfg()
        .cloud_topics_produce_cardinality_threshold.bind())
  , _scheduling_interval(
      config::shard_local_cfg().cloud_topics_produce_upload_interval.bind())
  , _probe(config::shard_local_cfg().disable_metrics())
  , _context(nullptr) {}

template<typename Clock>
ss::future<> write_request_scheduler<Clock>::start() {
    // Collect counters across all shards
    struct counter_shard {
        const std::atomic<size_t>* count;
        const std::atomic<size_t>* next_stage_count;
        ss::shard_id shard;
    };

    if (ss::this_shard_id() == ss::shard_id(0)) {
        auto shard_bytes = co_await this->container().map(
          [](write_request_scheduler<Clock>& s) {
              // Get the next stage bytes reference. This works even if the next
              // stage hasn't been registered yet because the counters are
              // pre-allocated.
              auto next_ref = s._stage.next_stage_bytes_ref();
              return counter_shard{
                .count = s._stage.stage_bytes_ref(),
                .next_stage_count = next_ref ? next_ref : &zero_counter,
                .shard = ss::this_shard_id(),
              };
          });
        // Create scheduler context on shard 0.
        // The context is shared between all shards.
        // FixedArrays are sized at construction time.
        _shard_zero_context.emplace(ss::smp::count);

        // Initialize shards with their backlog size references.
        // shard_to_group and groups are already default-initialized by
        // the FixedArray constructor (padded_atomic_group_id defaults to
        // group_id{0}, shard_group default-constructs with current time).
        for (unsigned ix = 0; ix < ss::smp::count; ix++) {
            for (const counter_shard& sc : shard_bytes) {
                if (sc.shard == ix) {
                    _shard_zero_context->shards[ix] = shard_state<Clock>(
                      std::ref(*sc.count), std::ref(*sc.next_stage_count));
                    break;
                }
            }
        }

        auto ref = &_shard_zero_context.value();

        // Install initialized context on all shards
        co_await this->container().invoke_on_all([ref](auto& service) {
            service._context = ref;
            service._init_barrier.signal();
        });
    } else {
        // Wait for shard 0 to initialize _context
        co_await _init_barrier.wait();
    }

    if (!_test_only_disable_background_loop) {
        ssx::spawn_with_gate(_gate, [this] { return bg_handler(); });
    }
    co_return;
}

template<typename Clock>
ss::future<> write_request_scheduler<Clock>::stop() {
    _as.request_abort();
    _init_barrier.broken();
    co_await _gate.close();
}

template<typename Clock>
ss::future<typename Clock::time_point>
write_request_scheduler<Clock>::run_once() {
    // This is a scheduler's "tick" which happens every
    // 5-20ms.
    auto this_shard = ss::this_shard_id();
    auto now = Clock::now();

    // Update active groups metric (only shard 0 owns the context data)
    if (this_shard == 0) {
        _probe.set_active_groups(_context->count_active_groups());
    }

    // Let scheduler_context make the scheduling decision
    auto result = _context->try_schedule_upload(
      this_shard, _max_buffer_size(), _scheduling_interval(), now, _probe);

    switch (result.action) {
    case schedule_action::skip:
        // Not this shard's turn
        co_return get_next_wakeup_time(false);

    case schedule_action::wait:
        // Conditions not met or mutex busy
        co_return get_next_wakeup_time(true);

    case schedule_action::upload: {
        // Collect shard info for shards in this group
        std::vector<shard_info> shard_bytes;
        shard_bytes.resize(ss::smp::count);
        _context->get_shard_bytes_vec(shard_bytes, result.gid);

        // Only shutdown exceptions are expected here
        co_await pull_and_roundtrip(
          std::move(shard_bytes), std::move(result.request));
        co_return get_next_wakeup_time(true);
    }
    }
    __builtin_unreachable();
}

template<typename Clock>
ss::future<> write_request_scheduler<Clock>::bg_handler() {
    auto projected_wakeup_time = get_next_wakeup_time(true);
    while (!_as.abort_requested()) {
        // Wait until max bytes or projected wake up time is reached. Check how
        // much data we have and was the deadline reached.
        auto event = co_await _stage.wait_until(
          _max_buffer_size(), projected_wakeup_time, &_as);
        if (!event.has_value()) {
            if (event.error() == errc::shutting_down) {
                vlog(cd_log.debug, "bg_handler: shutting down");
                co_return;
            } else {
                vlog(
                  cd_log.error,
                  "bg_handler: error waiting for write requests: {}",
                  event.error());
            }
            co_return;
        }
        projected_wakeup_time = co_await run_once();
    }
}

template<typename Clock>
write_request_scheduler<Clock>::time_point
write_request_scheduler<Clock>::get_next_wakeup_time(bool long_duration) {
    simple_time_jitter<Clock> jitter(
      long_duration ? scheduler_long_sleep_interval
                    : scheduler_short_sleep_interval,
      scheduler_sleep_jitter);
    auto now = Clock::now();
    auto soft_deadline = now + jitter.next_duration();

    // Use group's last upload time
    auto gid = _context->shard_to_group[ss::this_shard_id()].load();
    auto hard_deadline
      = _context->groups[static_cast<size_t>(gid)].last_upload_time.load()
        + jitter.next_jitter_duration();
    auto target = std::min(soft_deadline, hard_deadline);
    if (target < now) {
        // Special case. The upload failed so last upload time is
        // far in the past. If we will use "target" to schedule
        // next iteration we will be spin waiting for new data.
        // To avoid this always use soft deadline here.
        return soft_deadline;
    }
    return target;
}

template<typename Clock>
ss::future<> write_request_scheduler<Clock>::pull_and_roundtrip(
  std::vector<shard_info> infos,
  std::optional<schedule_request<Clock>> request) {
    // This method is invoked by the target shard to pull requests from
    // other shards and forward them to its own pipeline.
    std::vector<ss::future<>> in_flight;
    // 1. Schedule all other shards to forward their requests.
    // 2. Process own requests to trigger the signal.
    ss::gate target_gate;
    for (const auto& info : infos) {
        if (ss::this_shard_id() != info.shard) {
            // Forward requests to target shard by proxying
            // write requests
            if (info.bytes > 0) {
                // This is not conventional use of the gate. The unique_ptr is
                // used as a RAII container to pass this gate holder into the
                // 'roundtrip' method. The 'roundtrip' method will be invoked on
                // another shard. It guarantees that the gate holder will not be
                // destroyed while it's running on another shard. To make it
                // safe the pointer is wrapped with the foreign_ptr. Even if the
                // exception will be thrown while on another shard the gate
                // holder will be disposed on the target shard.
                //
                // The 'roundtrip' method will submit the continuation back to
                // the target shard that owns the gate and will eventually
                // destroy the holder. The target gate will be kept open until
                // all shards finish forwarding their requests. The foreign_ptr
                // has zero overhead when disposed on the original shard. It's
                // just a safety measure, not a correctness requirement.
                auto ptr = ss::make_foreign(
                  std::make_unique<ss::gate::holder>(target_gate.hold()));
                in_flight.push_back(this->container().invoke_on(
                  info.shard,
                  [target = ss::this_shard_id(), ptr = std::move(ptr)](
                    write_request_scheduler<Clock>& s) mutable {
                      return s.forward_to(target, std::move(ptr));
                  }));
            }
        }
    }
    // Wait until all shards have scheduled their requests.
    // This gate is only closed after every shard has forwarded all its
    // requests to the target shard's pipeline. This means that we can
    // notify the next pipeline stage about the requests and await futures
    // accumulated in the 'in_flight' vector. Instead of simply signalling
    // the gate we first forwarding target shard's own requests to the pipeline
    // which also signals the pipeline stage.
    // After the gate is closed the method awaits in-flight futures. Every
    // future awaits until the target shard uploads its write requests to the
    // cloud storage and then it acknowledges the source write requests (which
    // were proxied).
    co_await target_gate.close();
    auto gid = _context->shard_to_group[ss::this_shard_id()].load();
    _context->groups[static_cast<size_t>(gid)].last_upload_time.store(
      Clock::now());
    if (request.has_value()) {
        // Unlock will increment ix and release the mutex
        request.value().unlock();
    } else {
        vlog(cd_log.warn, "The cross shard state is not locked");
    }

    // Submit own requests to the pipeline
    bool signaled = false;
    for (const auto& info : infos) {
        if (ss::this_shard_id() == info.shard && info.bytes > 0) {
            // Fast path: process requests locally
            // This shard is the target shard selected by round-robin.
            _stage.process(
              [this, &signaled](const write_request<Clock>& r) noexcept {
                  signaled = true;
                  _probe.register_request(r.size_bytes());
                  return request_processing_result::advance_and_continue;
              });
            break;
        }
    }
    if (!signaled) {
        // The target shard is selected by round-robin. It is possible that the
        // selected shard has no local write requests. In this case the loop
        // above will see no write requests. Other shards are depositing their
        // requests to the target shard without signalling the next stage. So we
        // need to signal it here.
        _stage.signal_next_stage();
    }
    co_await ss::when_all_succeed(in_flight.begin(), in_flight.end());
}

/// Get a single write request which was created on another shard
/// then copy it and enqueue it to the pipeline on this shard.
/// Then await the response and return it.
template<typename Clock>
ss::future<
  std::expected<typename write_request_scheduler<Clock>::foreign_ptr_t, errc>>
write_request_scheduler<Clock>::proxy_write_request(
  write_request<Clock>* req, ss::gate::holder target_gate_holder) noexcept {
    // This is executed in the context of the target shard.
    // It is safe to dispose the gate holder here because
    // the holder was created on the target shard and
    // it will be destroyed on the target shard as well.
    auto h = _gate.hold();
    _probe.register_receive_xshard(req->size_bytes());
    // Create proxy for the foreign request. The bytes were already accounted
    // for on the source shard, so we use enqueue_foreign_request to place it
    // directly at the next stage without byte accounting.
    write_request<Clock> proxy(
      req->ntp,
      req->topic_start_epoch,
      shallow_copy(req->data_chunk),
      req->expiration_time);
    auto fut = proxy.response.get_future();
    _stage.enqueue_foreign_request(proxy, false);
    target_gate_holder.release();
    auto upload_fut = co_await ss::coroutine::as_future(std::move(fut));
    if (upload_fut.failed()) {
        auto ex = upload_fut.get_exception();
        vlog(cd_log.error, "Proxy write request failed: {}", ex);
        co_return std::unexpected(errc::upload_failure);
    }
    auto extents = upload_fut.get();
    if (!extents.has_value()) {
        // Normal errors (S3 upload failure or timeout)
        // are handled here
        errc e = extents.error();
        vlog(cd_log.info, "Proxy write request failed: {}", e);
        co_return std::unexpected(e);
    }
    auto ptr = ss::make_lw_shared<upload_meta>();
    *ptr = std::move(extents.value());
    foreign_ptr_t fp(ss::make_foreign(std::move(ptr)));
    co_return std::move(fp);
}

template<typename Clock>
void write_request_scheduler<Clock>::ack_write_response(
  write_request<Clock>* req,
  std::expected<write_request_scheduler<Clock>::foreign_ptr_t, errc> resp) {
    // The response was created on another shard.
    // The req was created on this shard.
    // The response contains only extent_meta struct so cheap
    // to copy between shards.
    if (!resp.has_value()) {
        req->set_value(resp.error());
    } else {
        req->set_value(std::move(*resp.value()));
    }
}

template<typename Clock>
ss::future<> write_request_scheduler<Clock>::roundtrip(
  ss::shard_id shard,
  write_pipeline<Clock>::write_requests_list list,
  ss::foreign_ptr<gate_holder_ptr> target_shard_gate_holder) {
    // This is executed in the context of the shard that owns the data.
    // The method submits the continuation back to the target shard
    // to complete the operation.
    auto h = _gate.hold();
    // Temporary storage for x-shard request and response correlation.
    using response_t
      = std::expected<write_request_scheduler<Clock>::foreign_ptr_t, errc>;
    struct result_t {
        std::optional<response_t> response{std::nullopt};
        write_request<Clock>* request{nullptr};
    };
    chunked_vector<result_t> results;
    for (auto& req : list.requests) {
        results.push_back(result_t{.request = &req});
        _probe.register_send_xshard(req.size_bytes());
    }
    co_await this->container().invoke_on(
      shard,
      [&results, gh = std::move(target_shard_gate_holder)](
        write_request_scheduler<Clock>& balancer) mutable {
          // This is executed on the target shard
          chunked_vector<ss::future<response_t>> futures;
          for (auto& r : results) {
              vassert(
                r.response.has_value() == false,
                "Should not have response yet");
              futures.emplace_back(balancer.proxy_write_request(
                r.request, /*copy gate holder*/ *gh));
          }
          gh->release();
          return ss::when_all(futures.begin(), futures.end())
            .then([&results](auto fut) {
                for (size_t i = 0; i < results.size(); i++) {
                    // This propagate the response or the error code.
                    // 'proxy_write_request' can't throw and the write_request
                    // can only be acknowledged using error code and not
                    // exception.
                    results[i].response = fut[i].get();
                }
            });
      });
    for (auto& r : results) {
        vassert(r.response.has_value(), "Should have response after invoke_on");
        ack_write_response(r.request, std::move(*r.response));
    }
}

template<typename Clock>
ss::future<> write_request_scheduler<Clock>::forward_to(
  ss::shard_id target_shard,
  ss::foreign_ptr<gate_holder_ptr> target_shard_gate_holder) {
    // Owning shard
    auto req = _stage.pull_write_requests(
      _max_buffer_size(), _max_cardinality());
    co_await roundtrip(
      target_shard, std::move(req), std::move(target_shard_gate_holder));
}

template class write_request_scheduler<seastar::lowres_clock>;
template class write_request_scheduler<seastar::manual_clock>;

template struct scheduler_context<seastar::lowres_clock>;
template struct scheduler_context<seastar::manual_clock>;

} // namespace cloud_topics::l0
