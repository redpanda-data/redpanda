// Prints the bytes Kafka's generated serializer produces for each consumer
// group coordinator record, as hex. These are the expected values in
// ../group_metadata_serialization_test.cc.
//
// Framing, per Kafka's GroupCoordinatorRecordSerde:
//   key   = int16 apiKey       + key message at version 0
//   value = int16 valueVersion + value message at that version
//
// Every field is set explicitly. The two sides disagree on defaults: Kafka's
// generator defaults a nullable string with no declared default to "", while
// the C++ defaults the corresponding optional to nullopt, and those encode
// differently (0x01 against 0x00).
//
// Building Kafka needs a JDK, not just a JRE, or :generator:compileJava fails
// with "does not provide the required capabilities: [JAVA_COMPILER]".
//
// clang-format off
//   cd "$KAFKA" && ./gradlew :group-coordinator:jar :clients:jar :server-common:jar
//   CP=$(find "$KAFKA" -path '*build/libs*' -name '*.jar' | tr '\n' ':')
//   javac -cp "$CP" -d /tmp/vectors GroupRecordVectors.java
//   java  -cp "$CP:/tmp/vectors" GroupRecordVectors
// clang-format on
//
// Each line of output is `<label> <hex>`, and the hex drops straight into the
// matching BOOST_REQUIRE_EQUAL in the test. Nothing re-runs this
// automatically, so re-run it when Kafka's ConsumerGroup*.json schemas change.
//
// The committed vectors, and therefore the encoding our codec is pinned to,
// were produced from Kafka at fce22525f7 (trunk, 4.4.0-SNAPSHOT). That commit
// is the baseline for "have the schemas changed": without it the instruction
// above has nothing to diff against.
//
// clang-format off
//   git -C "$KAFKA" diff fce22525f7..trunk -- \
//     group-coordinator/src/main/resources/common/message/'ConsumerGroup*.json'
// clang-format on

import java.util.List;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataValue;

public final class GroupRecordVectors {

  // The 16 bytes 01..10, matching the C++ test's topic_id.
  private static final Uuid TOPIC_ID
      = new Uuid(0x0102030405060708L, 0x090a0b0c0d0e0f10L);

  private static String hex(byte[] b) {
    StringBuilder sb = new StringBuilder(b.length * 2);
    for (byte x : b) {
      sb.append(String.format("%02x", x));
    }
    return sb.toString();
  }

  private static void emit(String label, short prefix, Message m) {
    System.out.printf(
        "%-56s %s%n", label,
        hex(MessageUtil.toVersionPrefixedBytes(prefix, m)));
  }

  public static void main(String[] args) {
    // --- apiKey 3: ConsumerGroupMetadata ---
    emit(
        "consumer_group_metadata_key{g1}", (short)3,
        new ConsumerGroupMetadataKey().setGroupId("g1"));

    emit(
        "consumer_group_metadata_value{epoch=7,hash=0}", (short)0,
        new ConsumerGroupMetadataValue().setEpoch(7).setMetadataHash(0L));

    emit(
        "consumer_group_metadata_value{epoch=7,hash=0x1122334455667788}",
        (short)0,
        new ConsumerGroupMetadataValue().setEpoch(7).setMetadataHash(
            0x1122334455667788L));

    // --- apiKey 5: ConsumerGroupMemberMetadata ---
    emit(
        "consumer_group_member_metadata_key{g1,m1}", (short)5,
        new ConsumerGroupMemberMetadataKey().setGroupId("g1").setMemberId(
            "m1"));

    emit(
        "consumer_group_member_metadata_value{c,h,[t],1000}", (short)0,
        new ConsumerGroupMemberMetadataValue()
            .setInstanceId(null)
            .setRackId(null)
            .setClientId("c")
            .setClientHost("h")
            .setSubscribedTopicNames(List.of("t"))
            .setSubscribedTopicRegex(null)
            .setRebalanceTimeoutMs(1000)
            .setServerAssignor(null)
            .setClassicMemberMetadata(null));

    // classic_metadata present: pins the presence-marker byte that precedes
    // the struct inside the tag payload.
    emit(
        "consumer_group_member_metadata_value{+classic}", (short)0,
        new ConsumerGroupMemberMetadataValue()
            .setInstanceId(null)
            .setRackId(null)
            .setClientId("c")
            .setClientHost("h")
            .setSubscribedTopicNames(List.of("t"))
            .setSubscribedTopicRegex(null)
            .setRebalanceTimeoutMs(1000)
            .setServerAssignor(null)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(30000)
                    .setSupportedProtocols(List.of(
                        new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                            .setName("range")
                            .setMetadata(new byte[] {1, 2})))));

    // --- apiKey 6: ConsumerGroupTargetAssignmentMetadata ---
    emit(
        "target_assignment_metadata_key{g1}", (short)6,
        new ConsumerGroupTargetAssignmentMetadataKey().setGroupId("g1"));

    emit(
        "target_assignment_metadata_value{epoch=3,ts=0}", (short)0,
        new ConsumerGroupTargetAssignmentMetadataValue()
            .setAssignmentEpoch(3)
            .setAssignmentTimestamp(0L));

    // --- apiKey 7: ConsumerGroupTargetAssignmentMember ---
    emit(
        "target_assignment_member_key{g1,m1}", (short)7,
        new ConsumerGroupTargetAssignmentMemberKey()
            .setGroupId("g1")
            .setMemberId("m1"));

    emit(
        "target_assignment_member_value{topic,[0]}", (short)0,
        new ConsumerGroupTargetAssignmentMemberValue().setTopicPartitions(
            List.of(
                new ConsumerGroupTargetAssignmentMemberValue.TopicPartition()
                    .setTopicId(TOPIC_ID)
                    .setPartitions(List.of(0)))));

    // --- apiKey 8: ConsumerGroupCurrentMemberAssignment ---
    emit(
        "current_member_assignment_key{g1,m1}", (short)8,
        new ConsumerGroupCurrentMemberAssignmentKey()
            .setGroupId("g1")
            .setMemberId("m1"));

    // Empty assignment_epochs: Kafka omits the tag.
    emit(
        "current_member_assignment_value{epochs=[],[0]}", (short)0,
        new ConsumerGroupCurrentMemberAssignmentValue()
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(0)
            .setState((byte)0)
            .setAssignedPartitions(List.of(
                new ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions()
                    .setTopicId(TOPIC_ID)
                    .setPartitions(List.of(0))
                    .setAssignmentEpochs(List.of())))
            .setPartitionsPendingRevocation(List.of()));

    // No assignment_epochs: the tagged field is absent.
    emit(
        "current_member_assignment_value{1,0,stable,[0]}", (short)0,
        new ConsumerGroupCurrentMemberAssignmentValue()
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(0)
            .setState((byte)0)
            .setAssignedPartitions(List.of(
                new ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions()
                    .setTopicId(TOPIC_ID)
                    .setPartitions(List.of(0))
                    .setAssignmentEpochs(null)))
            .setPartitionsPendingRevocation(List.of()));

    // Populated assignment_epochs, covering the nested struct's tagged field.
    emit(
        "current_member_assignment_value{epochs=[9],[5]}", (short)0,
        new ConsumerGroupCurrentMemberAssignmentValue()
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(0)
            .setState((byte)0)
            .setAssignedPartitions(List.of(
                new ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions()
                    .setTopicId(TOPIC_ID)
                    .setPartitions(List.of(5))
                    .setAssignmentEpochs(List.of(9))))
            .setPartitionsPendingRevocation(List.of()));
  }
}
