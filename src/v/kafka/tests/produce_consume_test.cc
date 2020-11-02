#include "kafka/client.h"
#include "kafka/errors.h"
#include "kafka/requests/fetch_request.h"
#include "kafka/requests/produce_request.h"
#include "kafka/requests/request_reader.h"
#include "random/generators.h"
#include "redpanda/tests/fixture.h"
#include "storage/record_batch_builder.h"
#include "test_utils/async.h"
#include "test_utils/fixture.h"

#include <boost/test/tools/old/interface.hpp>

using namespace std::chrono_literals;

struct prod_consume_fixture : public redpanda_thread_fixture {
    void start() {
        consumer = std::make_unique<kafka::client>(make_kafka_client().get0());
        producer = std::make_unique<kafka::client>(make_kafka_client().get0());
        consumer->connect().get0();
        producer->connect().get0();
        model::topic_namespace tp_ns(model::ns("kafka"), test_topic);
        add_topic(tp_ns).get0();
        model::ntp ntp(tp_ns.ns, tp_ns.tp, model::partition_id(0));
        tests::cooperative_spin_wait_with_timeout(2s, [ntp, this] {
            auto shard = app.shard_table.local().shard_for(ntp);
            if (!shard) {
                return ss::make_ready_future<bool>(false);
            }
            return app.partition_manager.invoke_on(
              *shard, [ntp](cluster::partition_manager& pm) {
                  return pm.get(ntp)->is_leader();
              });
        }).get0();
    }

    std::vector<kafka::produce_request::partition> small_batches(size_t count) {
        storage::record_batch_builder builder(
          model::well_known_record_batch_types[1], model::offset(0));

        for (int i = 0; i < count; ++i) {
            iobuf v{};
            v.append("v", 1);
            builder.add_raw_kv(iobuf{}, std::move(v));
        }

        std::vector<kafka::produce_request::partition> res;

        kafka::produce_request::partition partition;
        partition.id = model::partition_id(0);
        partition.adapter = kafka::kafka_batch_adapter();
        partition.adapter.batch = std::move(std::move(builder).build());
        res.push_back(std::move(partition));
        return res;
    }

    template<typename T>
    ss::future<> produce(T&& batch_factory) {
        kafka::produce_request::topic tp;
        size_t count = random_generators::get_int(1, 20);
        tp.partitions = batch_factory(count);
        tp.name = test_topic;
        std::vector<kafka::produce_request::topic> topics;
        topics.push_back(std::move(tp));
        kafka::produce_request req(std::nullopt, 1, std::move(topics));
        req.timeout = std::chrono::seconds(2);
        req.has_idempotent = false;
        req.has_transactional = false;
        return producer->dispatch(std::move(req))
          .then([](kafka::produce_response) {});
    }

    model::offset read_last_batch_offset(iobuf&& record_set) {
        kafka::request_reader rdr(std::move(record_set));
        auto bo = rdr.read_int64();
        auto len = rdr.read_int32();
        auto ple = rdr.read_int32();
        auto magic = rdr.read_int8();
        auto crc = rdr.read_int32();
        auto attr = rdr.read_int16();
        auto lod = rdr.read_int32();

        return model::offset(bo + lod);
    }

    ss::future<kafka::fetch_response> fetch_next() {
        kafka::fetch_request::partition partition;
        partition.fetch_offset = fetch_offset;
        partition.id = model::partition_id(0);
        partition.log_start_offset = model::offset(0);
        partition.partition_max_bytes = 1_MiB;
        kafka::fetch_request::topic topic;
        topic.name = test_topic;
        topic.partitions.push_back(partition);

        kafka::fetch_request req;
        req.min_bytes = 1;
        req.max_bytes = 10_MiB;
        req.max_wait_time = 100ms;
        req.topics.push_back(std::move(topic));

        return consumer->dispatch(std::move(req), kafka::api_version(4))
          .then([this](kafka::fetch_response resp) {
              if (resp.partitions.empty()) {
                  return resp;
              }
              auto& part = *resp.partitions.begin();

              for (auto& r : part.responses) {
                  std::optional<iobuf> data = std::move(
                    part.responses.begin()->record_set);
                  if (data && !data->empty()) {
                      // update next fetch offset the same way as Kafka clients
                      fetch_offset = read_last_batch_offset(std::move(*data))
                                     + model::offset(1);
                  }
              }
              return resp;
          });
    }

    model::offset fetch_offset{0};
    std::unique_ptr<kafka::client> consumer;
    std::unique_ptr<kafka::client> producer;
    ss::abort_source as;
    const model::topic test_topic = model::topic("test-topic");
};

/**
 * produce/consume test simulating Hazelcast benchmart workload with small
 * batches.
 */
FIXTURE_TEST(test_produce_consume_small_batches, prod_consume_fixture) {
    wait_for_controller_leadership().get0();
    start();
    produce([this](size_t cnt) { return small_batches(cnt); }).get0();
    auto resp_1 = fetch_next().get0();
    produce([this](size_t cnt) { return small_batches(cnt); }).get0();
    auto resp_2 = fetch_next().get0();

    BOOST_REQUIRE_EQUAL(resp_1.partitions.empty(), false);
    BOOST_REQUIRE_EQUAL(resp_2.partitions.empty(), false);
    BOOST_REQUIRE_EQUAL(resp_1.partitions.begin()->responses.empty(), false);
    BOOST_REQUIRE_EQUAL(
      resp_1.partitions.begin()->responses.begin()->error,
      kafka::error_code::none);
    BOOST_REQUIRE_EQUAL(resp_2.partitions.begin()->responses.empty(), false);
    BOOST_REQUIRE_EQUAL(
      resp_2.partitions.begin()->responses.begin()->error,
      kafka::error_code::none);
};
