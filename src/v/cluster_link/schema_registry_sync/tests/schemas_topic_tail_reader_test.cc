/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster_link/schema_registry_sync/schemas_topic_tail_reader.h"
#include "kafka/client/cluster.h"
#include "kafka/client/direct_consumer/tests/direct_consumer_fixture.h"
#include "kafka/client/test/cluster_mock.h"
#include "kafka/protocol/batch_reader.h"
#include "kafka/protocol/wire.h"
#include "model/compression.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "storage/record_batch_builder.h"
#include "test_utils/async.h"
#include "test_utils/scoped_config.h"

#include <seastar/core/abort_source.hh>

#include <gmock/gmock.h>

#include <ranges>
#include <vector>

using namespace std::chrono_literals;

using ::testing::AllOf;
using ::testing::Each;
using ::testing::Field;
using ::testing::IsEmpty;
using ::testing::Not;
using ::testing::Optional;
using ::testing::UnorderedElementsAre;

namespace cluster_link::schema_registry_sync {

namespace {

const auto& schemas_topic = ::model::schema_registry_internal_tp.topic;
constexpr auto test_idle_fetch_max_wait = 10ms;

// `_schemas` keys as a source writes them, spelled out as raw JSON rather than
// serialized from Redpanda's own key types, so the mapping is exercised against
// the wire format a foreign registry produces -- in particular keys with no
// `seq`/`node` fields, which only Redpanda writes.
ss::sstring schema_key(std::string_view subject, int version) {
    return ssx::sformat(
      R"({{"keytype":"SCHEMA","subject":"{}","version":{},"magic":1}})",
      subject,
      version);
}

ss::sstring subject_key(std::string_view keytype, std::string_view subject) {
    return ssx::sformat(
      R"({{"keytype":"{}","subject":"{}","magic":0}})", keytype, subject);
}

ss::sstring null_subject_key(std::string_view keytype) {
    return ssx::sformat(
      R"({{"keytype":"{}","subject":null,"magic":0}})", keytype);
}

// `count` versions of one subject, for a batch whose size matters more than its
// contents.
std::vector<ss::sstring> bulk_keys(std::string_view subject, int count) {
    return std::views::iota(0, count)
           | std::views::transform(
             [subject](int version) { return schema_key(subject, version); })
           | std::ranges::to<std::vector<ss::sstring>>();
}

// One batch carrying `keys`. The values are tombstones: the tail reads keys
// only, so this exercises the same path a populated record does.
::model::record_batch keys_batch(
  const std::vector<ss::sstring>& keys,
  ::model::compression compression = ::model::compression::none) {
    storage::record_batch_builder builder{
      ::model::record_batch_type::raft_data, ::model::offset{0}};
    builder.set_compression(compression);
    for (const auto& key : keys) {
        iobuf k;
        k.append(key.data(), key.size());
        builder.add_raw_kv(std::move(k), std::nullopt);
    }
    return std::move(builder).build();
}

// A batch whose header claims a codec its payload does not use. The checksum is
// recomputed over the header as altered, so the batch is well-formed on the
// wire and decoding is what fails -- what a source with a broken producer would
// send.
::model::record_batch mislabel_compression(::model::record_batch batch) {
    auto header = batch.header();
    header.attrs |= ::model::compression::zstd;
    auto payload = std::move(batch).release_data();
    header.reset_size_checksum_metadata(payload);
    return {header, std::move(payload), ::model::record_batch::tag_ctor_ng{}};
}

// `batch` as a fetch response carries it.
kafka::batch_reader wire_batch(::model::record_batch batch) {
    iobuf buf;
    kafka::protocol::encoder writer{buf};
    kafka::protocol::writer_serialize_batch(writer, std::move(batch));
    return kafka::batch_reader{std::move(buf)};
}

ppsr::context_subject cs(ppsr::context ctx, std::string_view sub) {
    return ppsr::context_subject{
      std::move(ctx), ppsr::subject{ss::sstring{sub}}};
}

ppsr::context_subject default_cs(std::string_view sub) {
    return cs(ppsr::default_context, sub);
}

// Matches a batch reporting exactly `subjects` and nothing else.
auto refreshes_subjects(auto... subjects) {
    return Optional(AllOf(
      Field(
        "subjects", &tail_batch::subjects, UnorderedElementsAre(subjects...)),
      Field("mode_configs", &tail_batch::mode_configs, IsEmpty()),
      Field("contexts", &tail_batch::contexts, IsEmpty())));
}

// Matches a batch reporting exactly `contexts` and nothing else.
auto reports_contexts(auto... contexts) {
    return Optional(AllOf(
      Field("subjects", &tail_batch::subjects, IsEmpty()),
      Field("mode_configs", &tail_batch::mode_configs, IsEmpty()),
      Field(
        "contexts", &tail_batch::contexts, UnorderedElementsAre(contexts...))));
}

/// Drives the reader against a real broker, so arming exercises the real
/// metadata path -- including that `_schemas`, a non-user topic, is visible to
/// a Kafka client at all.
class tail_reader_test
  : public kafka::client::tests::consumer_fixture
  , public ::testing::Test {
protected:
    void SetUp() override {
        create_node_application(::model::node_id{0});
        cluster = create_client_cluster().get();
        reader = std::make_unique<schemas_topic_tail_reader>(
          *cluster,
          schemas_topic_tail_reader::default_rearm_backoff,
          test_idle_fetch_max_wait);
    }

    void TearDown() override {
        reader->stop().get();
        cluster->stop().get();
    }

    void create_schemas_topic() {
        instance(::model::node_id{0})
          ->add_topic(
            {::model::kafka_namespace, schemas_topic}, 1, std::nullopt, 1)
          .get();
    }

    /// Writes `keys` to the source's `_schemas` as one batch.
    void write_keys(
      const std::vector<ss::sstring>& keys,
      ::model::compression compression = ::model::compression::none) {
        produce_to_partition(schemas_topic, 0, keys_batch(keys, compression))
          .get();
    }

    /// Polls until a batch reports something: the consumer's fetchers run in
    /// the background, so a write is not visible to the first poll.
    source_result<tail_batch> poll_until_nonempty() {
        source_result<tail_batch> polled = tail_batch{};
        ::tests::cooperative_spin_wait_with_timeout(30s, [this, &polled] {
            return reader->poll(as).then(
              [&polled](source_result<tail_batch> p) {
                  polled = std::move(p);
                  return !polled.has_value() || !polled->subjects.empty()
                         || !polled->mode_configs.empty()
                         || !polled->contexts.empty();
              });
        }).get();
        return polled;
    }

    std::unique_ptr<schemas_topic_tail_reader> reader;
    ss::abort_source as;
};

/// Drives the reader against a scripted source cluster. A real broker cannot be
/// made to fail a fetch terminally on demand: every error it produces here --
/// topic deletion included -- is retriable and handled inside the consumer's
/// own fetcher, so the reader's disarm path is unreachable without a mock.
class tail_reader_mock_test : public ::testing::Test {
protected:
    void SetUp() override {
        _mock.register_default_handlers();
        _mock.add_broker(
          ::model::node_id{0}, net::unresolved_address{"localhost", 9092});
        _mock.add_topic(schemas_topic, 1, 1);
        set_list_offsets_versions(
          kafka::list_offsets_api::min_valid,
          kafka::list_offsets_api::max_valid);
        _mock.register_handler(
          kafka::list_offsets_api::key,
          [this](
            ::model::node_id,
            kafka::client::request_t req,
            kafka::api_version) {
              ++list_offsets;
              auto listed = std::get<kafka::list_offsets_request>(
                std::move(req));
              // Mirrors the request's shape, so a reader that asked about more
              // than the one partition gets a reply it must reject.
              kafka::list_offset_response_data data;
              for (const auto& topic : listed.data.topics) {
                  kafka::list_offset_topic_response resp{.name = topic.name};
                  for (const auto& partition : topic.partitions) {
                      resp.partitions.push_back(
                        {.partition_index = partition.partition_index,
                         .offset = ::model::offset{0}});
                  }
                  data.topics.push_back(std::move(resp));
              }
              return ss::make_ready_future<kafka::client::response_t>(
                kafka::list_offsets_response{.data = std::move(data)});
          });
        _mock.register_handler(
          kafka::fetch_api::key,
          [this](
            ::model::node_id,
            kafka::client::request_t req,
            kafka::api_version) {
              auto fetched = std::get<kafka::fetch_request>(std::move(req));
              // Echoes the subscription, which the fetcher correlates responses
              // against, with `partition_error` as the verdict for each.
              kafka::fetch_response_data data;
              for (const auto& topic : fetched.data.topics) {
                  kafka::fetchable_topic_response resp{.topic = topic.topic};
                  for (const auto& partition : topic.partitions) {
                      fetch_offsets.push_back(partition.fetch_offset);
                      kafka::partition_data part{
                        .partition_index = partition.partition,
                        .error_code = partition_error};
                      if (
                        serve_undecodable
                        && partition.fetch_offset == ::model::offset{0}) {
                          part.high_watermark = ::model::offset{1};
                          part.records = wire_batch(mislabel_compression(
                            keys_batch({schema_key("orders-value", 1)})));
                      }
                      resp.partitions.push_back(std::move(part));
                  }
                  data.responses.push_back(std::move(resp));
              }
              return ss::make_ready_future<kafka::client::response_t>(
                kafka::fetch_response{.data = std::move(data)});
          });

        connect();
    }

    void TearDown() override {
        reader->stop().get();
        _cluster->stop().get();
    }

    /// The ListOffsets versions the source reports supporting, alongside the
    /// mock's defaults for the other APIs the client needs.
    void
    set_list_offsets_versions(kafka::api_version min, kafka::api_version max) {
        _mock.default_supported_versions[kafka::list_offsets_api::key] = {
          .min = min, .max = max};
    }

    /// Reconnects to a source reporting `min`..`max` for ListOffsets. A broker
    /// keeps the versions it reported when it was first connected to, so the
    /// connection has to be replaced rather than adjusted.
    void reconnect_reporting_list_offsets_versions(
      kafka::api_version min, kafka::api_version max) {
        reader->stop().get();
        _cluster->stop().get();
        set_list_offsets_versions(min, max);
        connect();
    }

    void connect() {
        _cluster = std::make_unique<kafka::client::cluster>(
          kafka::client::connection_configuration{
            .initial_brokers = {net::unresolved_address{"localhost", 9092}},
            .client_id = "test-client",
            .max_metadata_age = 1s,
          },
          std::make_unique<kafka::client::broker_mock_factory>(&_mock));
        _cluster->start().get();
        reader = std::make_unique<schemas_topic_tail_reader>(*_cluster);
    }

    kafka::client::cluster_mock _mock;
    std::unique_ptr<kafka::client::cluster> _cluster;
    std::unique_ptr<schemas_topic_tail_reader> reader;
    ss::abort_source as;
    /// What every fetched partition reports. Set mid-test to fail the tail.
    kafka::error_code partition_error{kafka::error_code::none};
    /// When set, every fetched partition also carries a batch that fetches
    /// cleanly but cannot be decoded.
    bool serve_undecodable{false};
    /// Arming offset lookups the source has served. Only a reader without a
    /// consumer resolves a position, so this counts consumers built.
    size_t list_offsets{0};
    /// Positions the source has been asked to fetch from, in order.
    std::vector<::model::offset> fetch_offsets;
};

} // namespace

TEST_F(tail_reader_test, unavailable_when_the_source_hides_the_topic) {
    // A source that does not expose `_schemas` (Confluent Cloud, Redpanda
    // Serverless) cannot be tailed, and that is not an error.
    EXPECT_EQ(reader->arm(as).get(), tail_availability::unavailable);

    // An unarmed reader polls empty rather than failing, so a tail tick needs
    // no availability check of its own.
    EXPECT_THAT(reader->poll(as).get(), refreshes_subjects());
}

TEST_F(tail_reader_test, re_arming_keeps_the_pinned_position) {
    create_schemas_topic();
    ASSERT_EQ(reader->arm(as).get(), tail_availability::available);

    // Written between two arms, so it belongs to the tail. Arming runs once per
    // full sync, and a second arm must keep the position rather than re-pin at
    // the log end: re-pinning would skip everything written since the previous
    // arm -- including changes the full sync in progress may already have read.
    write_keys({schema_key("between-arms-value", 1)});
    ASSERT_EQ(reader->arm(as).get(), tail_availability::available);

    EXPECT_THAT(
      poll_until_nonempty(),
      refreshes_subjects(default_cs("between-arms-value")));
}

TEST_F(tail_reader_test, arm_interrupted_by_stop_commits_no_consumer) {
    create_schemas_topic();

    // The task stops its readers while a run is still in flight, so an arm can
    // be suspended mid-round-trip when stop() lands. It must not go on to
    // publish the consumer it was building: nothing would ever stop that
    // consumer, and its background fetchers would keep running on the link's
    // Kafka connection after the reader was gone.
    //
    // Deterministic: arm() is suspended in its first source round-trip by the
    // time it hands back a future, and stop() raises its flag synchronously.
    auto armed = reader->arm(as);
    reader->stop().get();

    EXPECT_EQ(armed.get(), tail_availability::unavailable);
    EXPECT_THAT(reader->poll(as).get(), refreshes_subjects());
}

TEST_F(tail_reader_test, stop_while_polling_releases_the_consumer_safely) {
    create_schemas_topic();
    ASSERT_EQ(reader->arm(as).get(), tail_availability::available);

    // poll() parks on an empty fetch queue; stop() then releases the consumer
    // underneath it. Resuming must not touch what was released -- returning
    // empty and failing are both fine, using freed memory is not (this runs
    // under ASAN in CI).
    auto polled = reader->poll(as);
    reader->stop().get();
    std::move(polled)
      .then_wrapped([](ss::future<source_result<tail_batch>> f) {
          f.ignore_ready_future();
      })
      .get();

    EXPECT_THAT(reader->poll(as).get(), refreshes_subjects());
}

TEST_F(tail_reader_test, arming_pins_the_position_at_the_topic_end) {
    create_schemas_topic();
    // Written before arming, so the full sync that arms the reader has already
    // covered it and the tail must not replay it.
    write_keys({schema_key("before-arming-value", 1)});
    ASSERT_EQ(reader->arm(as).get(), tail_availability::available);

    write_keys({schema_key("after-arming-value", 1)});
    EXPECT_THAT(
      poll_until_nonempty(),
      refreshes_subjects(default_cs("after-arming-value")));
}

TEST_F(tail_reader_test, schema_records_refresh_their_subject) {
    create_schemas_topic();
    ASSERT_EQ(reader->arm(as).get(), tail_availability::available);

    write_keys(
      {schema_key("orders-value", 1),
       schema_key("orders-value", 2),
       schema_key(":.prod:payments-value", 1)});

    // Two versions of one subject collapse into a single refresh.
    EXPECT_THAT(
      poll_until_nonempty(),
      refreshes_subjects(
        default_cs("orders-value"),
        cs(ppsr::context{".prod"}, "payments-value")));
}

TEST_F(tail_reader_test, subject_deletes_refresh_their_subject) {
    create_schemas_topic();
    ASSERT_EQ(reader->arm(as).get(), tail_availability::available);

    write_keys({subject_key("DELETE_SUBJECT", "orders-value")});

    EXPECT_THAT(
      poll_until_nonempty(), refreshes_subjects(default_cs("orders-value")));
}

TEST_F(tail_reader_test, config_and_mode_records_refresh_their_target) {
    create_schemas_topic();
    ASSERT_EQ(reader->arm(as).get(), tail_availability::available);

    write_keys(
      {subject_key("CONFIG", "orders-value"),
       subject_key("MODE", ":.prod:"),
       // A null subject names the default context's target -- how `PUT /config`
       // and `PUT /mode` serialize theirs, for pre-context compatibility.
       null_subject_key("CONFIG"),
       // The registry-wide target names itself explicitly.
       subject_key("MODE", ":.__GLOBAL:")});

    EXPECT_THAT(
      poll_until_nonempty(),
      Optional(AllOf(
        Field(&tail_batch::subjects, IsEmpty()),
        Field(
          &tail_batch::mode_configs,
          UnorderedElementsAre(
            default_cs("orders-value"),
            cs(ppsr::context{".prod"}, ""),
            default_cs(""),
            ppsr::global_mode_config_target)))));
}

TEST_F(tail_reader_test, qualified_keys_parse_qualified_when_disabled_locally) {
    // `_schemas` keys follow the source's qualified-subject convention; the
    // destination's own schema_registry_enable_qualified_subjects must not
    // change how they read, or a context-remapping link on a destination with
    // it disabled would flatten every tailed subject into a default-context
    // literal that its scope filter then drops.
    scoped_config cfg;
    cfg.get("schema_registry_enable_qualified_subjects").set_value(false);

    create_schemas_topic();
    ASSERT_EQ(reader->arm(as).get(), tail_availability::available);

    write_keys(
      {schema_key(":.prod:orders-value", 1),
       subject_key("CONFIG", ":.prod:orders-value")});

    EXPECT_THAT(
      poll_until_nonempty(),
      Optional(AllOf(
        Field(
          &tail_batch::subjects,
          UnorderedElementsAre(cs(ppsr::context{".prod"}, "orders-value"))),
        Field(
          &tail_batch::mode_configs,
          UnorderedElementsAre(cs(ppsr::context{".prod"}, "orders-value"))))));
}

TEST_F(tail_reader_test, context_records_report_their_context) {
    create_schemas_topic();
    ASSERT_EQ(reader->arm(as).get(), tail_availability::available);

    // A CONTEXT record names no subject, so it implies no refresh -- it only
    // tells the caller the source's set of contexts may have changed, which is
    // what makes a source-side context delete observable without a full sync.
    write_keys(
      {R"({"keytype":"CONTEXT","context":".prod","magic":0})",
       R"({"keytype":"CONTEXT","context":".staging","magic":0})"});

    EXPECT_THAT(
      poll_until_nonempty(),
      reports_contexts(ppsr::context{".prod"}, ppsr::context{".staging"}));
}

TEST_F(tail_reader_test, records_implying_no_refresh_are_skipped) {
    create_schemas_topic();
    ASSERT_EQ(reader->arm(as).get(), tail_availability::available);

    // NOOP (which Confluent's kafkastore writes on leader election), a CONTEXT
    // record naming no context, a SCHEMA record naming no subject, an unknown
    // keytype naming no subject, a key that is not JSON at all, and keys that
    // are JSON but not objects: none implies a refresh, and none may abort the
    // batch.
    write_keys(
      {R"({"keytype":"NOOP","magic":0})",
       R"({"keytype":"CONTEXT","magic":0})",
       R"({"keytype":"SCHEMA","magic":0})",
       R"({"keytype":"WAT","magic":0})",
       "not json at all",
       "[]",
       R"("SCHEMA")",
       schema_key("orders-value", 1)});

    // The one real record still lands, proving the rest were skipped rather
    // than failing the poll.
    EXPECT_THAT(
      poll_until_nonempty(), refreshes_subjects(default_cs("orders-value")));
}

TEST_F(
  tail_reader_test, unknown_subject_bearing_keytypes_refresh_their_subject) {
    create_schemas_topic();
    ASSERT_EQ(reader->arm(as).get(), tail_availability::available);

    // A keytype Redpanda does not model but which names a subject -- Confluent
    // writes such keys (CLEAR_SUBJECT on a permanent subject delete). The named
    // subject is refreshed rather than the change waiting for the next full
    // sync; a nested "subject" inside some other field's value does not count.
    write_keys(
      {subject_key("CLEAR_SUBJECT", "orders-value"),
       R"({"keytype":"WAT","payload":{"subject":"not-a-target"},"magic":0})"});

    EXPECT_THAT(
      poll_until_nonempty(), refreshes_subjects(default_cs("orders-value")));
}

TEST_F(tail_reader_test, compressed_batches_are_decoded) {
    create_schemas_topic();
    ASSERT_EQ(reader->arm(as).get(), tail_availability::available);

    write_keys(
      {schema_key("orders-value", 1),
       subject_key("DELETE_SUBJECT", "payments-value")},
      ::model::compression::zstd);

    EXPECT_THAT(
      poll_until_nonempty(),
      refreshes_subjects(
        default_cs("orders-value"), default_cs("payments-value")));
}

TEST_F(tail_reader_test, poll_budget_truncates_and_the_next_poll_resumes) {
    create_schemas_topic();
    ASSERT_EQ(reader->arm(as).get(), tail_availability::available);

    // Six batches of ~0.9MiB against the 1MiB per-poll budget. A fetch stops
    // only once it is *over* max_bytes, so one response carries two batches and
    // blows the budget on its own: the poll that takes it must report truncated
    // with the rest still pending. Each batch's keys share one distinct
    // subject, so the batches a poll decoded are exactly the subjects it
    // reports.
    constexpr int n_batches = 6;
    constexpr int keys_per_batch = 900;
    const ss::sstring padding(1000, 'x');
    for (const auto b : std::views::iota(0, n_batches)) {
        write_keys(
          bulk_keys(ssx::sformat("bulk-{}-{}", b, padding), keys_per_batch));
    }

    chunked_hash_set<ppsr::context_subject> collected;
    bool saw_truncated_with_remainder = false;
    ::tests::cooperative_spin_wait_with_timeout(30s, [&] {
        return reader->poll(as).then([&](source_result<tail_batch> polled) {
            if (!polled.has_value()) {
                return true;
            }
            for (const auto& sub : polled->subjects) {
                collected.insert(sub);
            }
            // A budget-cut poll must leave batches behind for the next one.
            if (polled->truncated && collected.size() < n_batches) {
                saw_truncated_with_remainder = true;
            }
            return collected.size() == n_batches;
        });
    }).get();

    EXPECT_EQ(collected.size(), n_batches);
    EXPECT_TRUE(saw_truncated_with_remainder);
}

TEST_F(tail_reader_mock_test, a_terminal_fetch_error_disarms_tailing) {
    ASSERT_EQ(reader->arm(as).get(), tail_availability::available);
    ASSERT_EQ(list_offsets, 1);
    // A reader that holds its position resolves no new one, which is what makes
    // the count below evidence of a released consumer rather than of arming.
    ASSERT_EQ(reader->arm(as).get(), tail_availability::available);
    ASSERT_EQ(list_offsets, 1);

    // Authorization is the class of error that reaches a poll at all: the
    // fetcher retries everything transient itself (leadership moves, unknown
    // topic, network), so what surfaces here is what the source keeps
    // returning.
    partition_error = kafka::error_code::topic_authorization_failed;

    // The fetcher delivers that error asynchronously, so poll until the reader
    // has taken it -- observable because arming then has to build a new
    // consumer, which a reader still holding one would not do.
    ::tests::cooperative_spin_wait_with_timeout(30s, [this] {
        return reader->poll(as).then([this](source_result<tail_batch> polled) {
            EXPECT_TRUE(polled.has_value());
            return reader->arm(as).then([this](tail_availability availability) {
                EXPECT_EQ(availability, tail_availability::available);
                return list_offsets > 1;
            });
        });
    }).get();

    EXPECT_EQ(list_offsets, 2);
}

TEST_F(tail_reader_mock_test, unavailable_when_the_source_is_too_old_to_ask) {
    // Below v4 a ListOffsets reply need not carry the offset field this reads
    // at all, and the request's leader-epoch fencing is dropped on the wire, so
    // such a source is left to full syncs rather than tailed from a position it
    // never actually reported.
    reconnect_reporting_list_offsets_versions(
      kafka::api_version{0}, kafka::api_version{0});

    EXPECT_EQ(reader->arm(as).get(), tail_availability::unavailable);
    EXPECT_THAT(reader->poll(as).get(), refreshes_subjects());
    EXPECT_EQ(list_offsets, 0);
}

TEST_F(tail_reader_mock_test, arms_against_a_source_at_the_oldest_version) {
    // The floor is inclusive: a source capped at exactly v4 is still tailed.
    reconnect_reporting_list_offsets_versions(
      kafka::api_version{4}, kafka::api_version{4});

    EXPECT_EQ(reader->arm(as).get(), tail_availability::available);
    EXPECT_EQ(list_offsets, 1);
}

TEST_F(tail_reader_mock_test, an_undecodable_batch_is_skipped) {
    ASSERT_EQ(reader->arm(as).get(), tail_availability::available);
    ASSERT_EQ(list_offsets, 1);

    // A batch whose contents do not match its header would throw out of the
    // decode. It must cost only its own records: the poll still reports, and
    // the reader stays armed, which a re-arm shows by needing no new consumer
    // -- it would have had to list offsets again for one.
    serve_undecodable = true;

    ::tests::cooperative_spin_wait_with_timeout(30s, [this] {
        return reader->poll(as).then([this](source_result<tail_batch> polled) {
            EXPECT_TRUE(polled.has_value());
            return !fetch_offsets.empty()
                   && fetch_offsets.back() > ::model::offset{0};
        });
    }).get();

    EXPECT_EQ(reader->arm(as).get(), tail_availability::available);
    EXPECT_EQ(list_offsets, 1);
}

TEST_F(tail_reader_mock_test, poll_resumes_a_tail_once_the_backoff_elapses) {
    reader = std::make_unique<schemas_topic_tail_reader>(*_cluster, 0ms);
    ASSERT_EQ(reader->arm(as).get(), tail_availability::available);
    ASSERT_EQ(list_offsets, 1);

    partition_error = kafka::error_code::topic_authorization_failed;
    ::tests::cooperative_spin_wait_with_timeout(30s, [this] {
        return reader->poll(as).then(
          [](source_result<tail_batch> polled) { return polled.has_value(); });
    }).get();
    partition_error = kafka::error_code::none;

    // Resuming reuses the stored position, so it resolves no new one: the count
    // staying put is what tells a revived tail from a re-armed one.
    for (int poll = 0; poll < 3; ++poll) {
        EXPECT_TRUE(reader->poll(as).get().has_value());
        EXPECT_EQ(reader->arm(as).get(), tail_availability::available);
        EXPECT_EQ(list_offsets, 1);
    }
}

TEST_F(tail_reader_mock_test, poll_does_not_resume_a_reader_that_never_armed) {
    // Nothing to resume from, which is what keeps a source that cannot be
    // tailed at all off this path instead of retrying it every poll.
    reader = std::make_unique<schemas_topic_tail_reader>(*_cluster, 0ms);

    EXPECT_TRUE(reader->poll(as).get().has_value());
    EXPECT_EQ(list_offsets, 0);
    EXPECT_THAT(fetch_offsets, IsEmpty());
}

TEST_F(tail_reader_mock_test, rewinding_before_any_poll_keeps_the_position) {
    ASSERT_EQ(reader->arm(as).get(), tail_availability::available);

    // Nothing has been read, so there is nothing to put back -- and dropping
    // the consumer for that would cost the position it just armed at.
    reader->rewind().get();

    EXPECT_EQ(reader->arm(as).get(), tail_availability::available);
    EXPECT_EQ(list_offsets, 1);
}

TEST_F(tail_reader_mock_test, a_stopped_reader_does_not_resume) {
    reader = std::make_unique<schemas_topic_tail_reader>(*_cluster, 0ms);
    ASSERT_EQ(reader->arm(as).get(), tail_availability::available);

    // Reviving a consumer after stop() would leave its fetchers on the link's
    // connection past the reader's own shutdown.
    reader->stop().get();
    fetch_offsets.clear();

    EXPECT_TRUE(reader->poll(as).get().has_value());
    EXPECT_THAT(fetch_offsets, IsEmpty());
}

TEST_F(tail_reader_mock_test, a_resume_reads_from_after_the_last_batch_seen) {
    reader = std::make_unique<schemas_topic_tail_reader>(*_cluster, 0ms);
    ASSERT_EQ(reader->arm(as).get(), tail_availability::available);
    ASSERT_THAT(fetch_offsets, Each(::model::offset{0}));

    // Serve a batch the decode cannot read. It still counts as seen -- resuming
    // onto it would only skip it again -- and it ends at offset 0.
    serve_undecodable = true;
    ::tests::cooperative_spin_wait_with_timeout(30s, [this] {
        return reader->poll(as).then([this](source_result<tail_batch> polled) {
            EXPECT_TRUE(polled.has_value());
            return fetch_offsets.back() > ::model::offset{0};
        });
    }).get();

    // With the fetch failing terminally and no backoff, every poll disarms and
    // resumes, and nothing can advance the position further. So every fetch
    // from here is a freshly assigned consumer's first, reading from where the
    // reader itself had got to -- past the batch it could not decode.
    serve_undecodable = false;
    partition_error = kafka::error_code::topic_authorization_failed;
    for (int poll = 0; poll < 3; ++poll) {
        reader->poll(as).get();
    }
    fetch_offsets.clear();
    for (int poll = 0; poll < 3; ++poll) {
        reader->poll(as).get();
    }

    EXPECT_THAT(fetch_offsets, AllOf(Not(IsEmpty()), Each(::model::offset{1})));
}

} // namespace cluster_link::schema_registry_sync
