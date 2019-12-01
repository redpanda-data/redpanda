#include "librdkafka/rdkafkacpp.h"
#include "redpanda/tests/fixture.h"
#include "test_utils/fixture.h"

#include <cppkafka/cppkafka.h>
#include <v/native_thread_pool.h>

seastar::future<> get_metadata(v::ThreadPool& tp) {
    return tp.submit([]() {
        cppkafka::Configuration config = {
          {"metadata.broker.list", "127.0.0.1:9092"}};

        cppkafka::Producer producer(config);
        cppkafka::Metadata metadata = producer.get_metadata();

        if (!metadata.get_topics().empty()) {
            throw new std::runtime_error("expected topic set to be empty");
        }
    });
}

FIXTURE_TEST(get_metadadata, redpanda_thread_fixture) {
    v::ThreadPool thread_pool(1, 1, 0);
    thread_pool.start().get();
    get_metadata(thread_pool).get();
    thread_pool.stop().get();
}
