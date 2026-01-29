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

#include "kafka/client/direct_consumer/tests/direct_consumer_fixture.h"

#include "kafka/client/direct_consumer/fetcher.h"
#include "kafka/server/tests/produce_consume_utils.h"
#include "ssx/single_fiber_executor.h"

#include <chrono>
#include <exception>
#include <stdexcept>
#include <type_traits>
#include <utility>

ss::logger logger{"direct-consumer-fixture"};

namespace kafka::client::tests {
kafka::client::connection_configuration
consumer_fixture::make_connection_config() {
    kafka::client::connection_configuration config;
    config.client_id = "test-client";
    config.initial_brokers
      = std::views::transform(
          instance_ids(), [this](model::node_id id) { return instance(id); })
        | std::views::transform(
          [](const redpanda_thread_fixture* f) { return f->kafka_port; })
        | std::views::transform(
          [](int port) { return net::unresolved_address("localhost", port); })
        | std::ranges::to<std::vector<net::unresolved_address>>();
    return config;
}

ss::future<std::unique_ptr<kafka::client::cluster>>
consumer_fixture::create_client_cluster(
  std::optional<std::unique_ptr<kafka::client::broker_factory>>
    broker_factory) {
    auto cluster = [&]() {
        if (broker_factory.has_value()) {
            vassert(
              broker_factory.value() != nullptr,
              "expected non-null broker factory if provided");
            return std::make_unique<kafka::client::cluster>(
              make_connection_config(), std::move(broker_factory.value()));
        }
        return std::make_unique<kafka::client::cluster>(
          make_connection_config());
    }();
    co_await cluster->start();
    co_return cluster;
}

topic_assignment consumer_fixture::make_assignment(
  const model::topic& topic,
  std::vector<int> partitions,
  std::optional<kafka::offset> initial_offset) {
    topic_assignment assignment;
    assignment.topic = topic;
    for (const auto& p : partitions) {
        partition_assignment part;
        part.partition_id = model::partition_id(p);
        part.next_offset = initial_offset;
        assignment.partitions.push_back(part);
    }
    return assignment;
}

ss::future<model::node_id>
consumer_fixture::get_partition_leader_async(const model::ntp& ntp) {
    std::optional<model::node_id> leader_id;
    model::timeout_clock::time_point deadline = model::timeout_clock::now()
                                                + 10s;
    while (!leader_id) {
        if (model::timeout_clock::now() > deadline) {
            throw std::runtime_error(
              fmt::format("Timeout while waiting for leader for {}", ntp));
        }
        leader_id = instance(model::node_id{0})
                      ->app.controller->get_partition_leaders()
                      .local()
                      .get_leader(ntp);

        co_await ss::sleep(100ms);
    }
    if (!leader_id) {
        throw std::runtime_error(
          fmt::format("never found leader for ntp: {}", ntp));
    }
    co_return leader_id.value();
}

model::node_id consumer_fixture::get_partition_leader(const model::ntp& ntp) {
    return get_partition_leader_async(ntp).get();
}

ss::future<kafka::offset> consumer_fixture::produce_to_partition(
  const model::topic& topic,
  int partition,
  model::record_batch batch,
  std::chrono::milliseconds timeout,
  std::chrono::milliseconds backoff) {
    // bounce the captures into a coroutine frame
    auto do_produce =
      [this, topic, partition, batch = std::move(batch)](ss::abort_source&)
      -> ss::future<std::expected<kafka::offset, std::runtime_error>> {
        // produce to partition, throws runtime_error for expected produce
        // errors
        auto do_produce_async
          = +[](
               consumer_fixture* self,
               model::topic topic,
               int partition,
               model::record_batch batch) -> ss::future<kafka::offset> {
            model::ntp ntp(
              model::kafka_namespace, topic, model::partition_id(partition));
            auto leader_id = co_await self->get_partition_leader_async(ntp);

            vlog(
              logger.debug,
              "[broker: {}] Produce records to {}",
              leader_id,
              ntp);

            auto transport
              = co_await self->instance(leader_id)->make_kafka_client();
            auto client = ::tests::kafka_produce_transport(
              std::move(transport));
            co_await client.start();
            co_return co_await client
              .produce_to_partition(
                topic, model::partition_id(partition), std::move(batch))
              .finally([&client] { return client.stop(); });
        };

        // map runtime error into expected, will retry on runtime error
        return do_produce_async(this, topic, partition, batch.copy())
          .then_wrapped(
            [](ss::future<kafka::offset> f)
              -> std::expected<kafka::offset, std::runtime_error> {
                if (!f.failed()) {
                    return f.get();
                }
                try {
                    std::rethrow_exception(f.get_exception());
                } catch (const std::runtime_error& e) {
                    vlog(
                      logger.info,
                      "failed to produce with error: {} will retry",
                      e);
                    return std::unexpected(e);
                }
                std::unreachable();
            });
    };

    auto stop = [](std::expected<kafka::offset, std::runtime_error> out) {
        return out.has_value() ? ss::stop_iteration::yes
                               : ss::stop_iteration::no;
    };

    ss::abort_source as;
    retry_chain_node parent_rcn{as, timeout, backoff, retry_strategy::polling};
    auto retry = ssx::repeater_with_rcn(do_produce, stop, &parent_rcn);
    auto result = co_await retry(as);

    // bubble up the last error if all retries exhausted
    if (!result.has_value()) {
        throw result.error();
    }
    co_return result.value();
}

ss::future<> consumer_fixture::produce_to_partition(
  const model::topic& topic,
  int partition,
  size_t record_count,
  std::chrono::milliseconds timeout,
  std::chrono::milliseconds backoff) {
    // bounce the captures into a coroutine frame
    auto do_produce = [this, topic, partition, record_count](ss::abort_source&)
      -> ss::future<std::expected<void, std::runtime_error>> {
        // produce to partition, throws runtime_error for expected produce
        // errors
        auto do_produce_async = +[](
                                   consumer_fixture* self,
                                   model::topic topic,
                                   int partition,
                                   size_t record_count) -> ss::future<void> {
            model::ntp ntp(
              model::kafka_namespace, topic, model::partition_id(partition));
            auto leader_id = co_await self->get_partition_leader_async(ntp);
            vlog(
              logger.debug,
              "[broker: {}] Produce {} records to {}",
              leader_id,
              record_count,
              ntp);

            auto transport
              = co_await self->instance(leader_id)->make_kafka_client();
            auto client = ::tests::kafka_produce_transport(
              std::move(transport));
            co_await client.start();
            co_await client
              .produce_to_partition(
                topic,
                model::partition_id(partition),
                ::tests::kv_t::sequence(0, record_count, partition))
              .finally([&client] { return client.stop(); });
        };

        // map runtime error into expected, will retry on runtime error
        return do_produce_async(this, topic, partition, record_count)
          .then_wrapped(
            [](ss::future<void> f) -> std::expected<void, std::runtime_error> {
                if (!f.failed()) {
                    return {};
                }
                try {
                    std::rethrow_exception(f.get_exception());
                } catch (const std::runtime_error& e) {
                    vlog(
                      logger.info,
                      "failed to produce with error: {} will retry",
                      e);
                    return std::unexpected(e);
                }
                std::unreachable();
            });
    };

    auto stop = [](std::expected<void, std::runtime_error> out) {
        return out.has_value() ? ss::stop_iteration::yes
                               : ss::stop_iteration::no;
    };

    ss::abort_source as;
    retry_chain_node parent_rcn{as, timeout, backoff, retry_strategy::polling};
    auto retry = ssx::repeater_with_rcn(do_produce, stop, &parent_rcn);
    auto result = co_await retry(as);

    // bubble up the last error if all retries exhausted
    if (!result.has_value()) {
        throw result.error();
    }
}

ss::future<chunked_vector<model::record>>
consumer_fixture::consume_from_partition(
  const model::topic& topic, int partition, kafka::offset offset) {
    model::ntp ntp(
      model::kafka_namespace, topic, model::partition_id(partition));
    auto leader_id = get_partition_leader(ntp);
    vlog(
      logger.debug,
      "[broker: {}] Consuming records from partition {}, offset: {}",
      leader_id,
      ntp,
      offset);

    return instance(leader_id)->make_kafka_client().then(
      [topic, partition, offset](auto transport) mutable {
          return ss::do_with(
            ::tests::kafka_consume_transport(std::move(transport)),
            [topic, partition, offset](auto& consumer) {
                return consumer.start().then(
                  [&consumer, topic, partition, offset] {
                      return consumer
                        .raw_consume_from_partition(
                          topic,
                          model::partition_id(partition),
                          kafka::offset_cast(offset))
                        .finally([&consumer] { return consumer.stop(); });
                  });
            });
      });
}

chunked_hash_map<model::topic_partition, chunked_vector<model::record_batch>>
consumer_fixture::fetch_until_empty(direct_consumer& consumer) {
    chunked_hash_map<
      model::topic_partition,
      chunked_vector<model::record_batch>>
      ret;

    while (true) {
        auto fetched = consumer.fetch_next(1000ms).get();

        if (fetched.value().empty()) {
            break;
        }
        for (auto& topic_data : fetched.value()) {
            for (auto& partition_data : topic_data.partitions) {
                auto& batches = ret[model::topic_partition(
                  topic_data.topic, partition_data.partition_id)];

                std::ranges::move(
                  partition_data.data, std::back_inserter(batches));
            }
        }
    }
    return ret;
}

void consumer_fixture::assign_partitions(topic_assignment assgn) {
    consumer
      ->assign_partitions(
        chunked_vector<topic_assignment>::single(std::move(assgn)))
      .get();
}

void consumer_fixture::unassign_partition(model::topic_partition tp) {
    consumer
      ->unassign_partitions(
        chunked_vector<model::topic_partition>::single(std::move(tp)))
      .get();
}

void consumer_fixture::unassign_topic(model::topic topic) {
    consumer
      ->unassign_topics(chunked_vector<model::topic>::single(std::move(topic)))
      .get();
}

// shuffle leadership, wait for leadership change to become visible to the
// test
void consumer_fixture::wait_for_visible_leadership_shuffle(
  const model::ntp& ntp) {
    constexpr auto leadership_swap_wait = 300ms;
    const auto original_leader = get_leader(ntp);
    auto current_leader = original_leader;

    shuffle_leadership(ntp).get();

    for (const auto iteration : std::array{0, 1, 2, 3, 4, 5}) {
        logger.info("polling for leadership change iteration: {}", iteration);
        current_leader = get_leader(ntp);
        ss::sleep(leadership_swap_wait).get();
        if (current_leader != original_leader) {
            break;
        }
    }
    ASSERT_NE(current_leader, original_leader)
      << "never detected leadership change, test precondition failed";
}

application* consumer_fixture::create_node_application(model::node_id node_id) {
    return cluster_test_fixture::create_node_application(
      node_id,
      9092,
      11000,
      std::nullopt,
      std::nullopt,
      configure_node_id::yes,
      empty_seed_starts_cluster::yes,
      std::nullopt,
      std::nullopt,
      std::nullopt,
      true,
      false,
      false,
      /* cluster_linking_enabled */ true);
}

std::unique_ptr<kafka::client::direct_consumer>
basic_consumer_fixture::make_consumer() {
    return std::make_unique<kafka::client::direct_consumer>(
      *cluster,
      direct_consumer::configuration{
        .with_sessions = fetch_sessions_enabled{
          GetParam() == session_config::with_sessions}});
}

void basic_consumer_fixture::SetUp() {
    create_node_application(model::node_id{0});
    create_node_application(model::node_id{1});
    create_node_application(model::node_id{2});
    auto* rp = instance(model::node_id{0});
    wait_for_all_members(3s).get();
    rp->add_topic({model::kafka_namespace, topic}, 3, std::nullopt, 3).get();
    cluster = create_client_cluster().get();
    StartConsumer();
}

void basic_consumer_fixture::StartConsumer() {
    consumer = make_consumer();
    consumer->start().get();
}

void basic_consumer_fixture::TearDown() {
    StopConsumer();
    cluster->stop().get();
}

void basic_consumer_fixture::StopConsumer() {
    if (consumer) {
        consumer->stop().get();
    }
}

void basic_consumer_fixture::maybe_toggle_fetch_sessions() {
    if (GetParam() == session_config::toggle_sessions) {
        consumer->update_configuration(
          direct_consumer::configuration{
            .with_sessions = fetch_sessions_enabled::yes});
    }
}

} // namespace kafka::client::tests
