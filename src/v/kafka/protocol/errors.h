/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include <cstdint>
#include <iosfwd>
#include <string_view>
#include <system_error>

namespace kafka {

enum class error_code : int16_t {
    // The server experienced an unexpected error when processing the request.
    unknown_server_error = -1,
    none = 0,
    // The requested offset is not within the range of offsets maintained by the
    // server.
    offset_out_of_range = 1,
    // This message has failed its CRC checksum, exceeds the valid size, has a
    // null key for a compacted topic, or is otherwise corrupt.
    corrupt_message = 2,
    // This server does not host this topic-partition.
    unknown_topic_or_partition = 3,
    // The requested fetch size is invalid.
    invalid_fetch_size = 4,
    // There is no leader for this topic-partition as we are in the middle of a
    // leadership election.
    leader_not_available = 5,
    // This server is not the leader for that topic-partition.
    not_leader_for_partition = 6,
    // The request timed out.
    request_timed_out = 7,
    // The broker is not available.
    broker_not_available = 8,
    // The replica is not available for the requested topic-partition.
    replica_not_available = 9,
    // The request included a message larger than the max message size the
    // server will accept.
    message_too_large = 10,
    // The controller moved to another broker.
    stale_controller_epoch = 11,
    // The metadata field of the offset request was too large.
    offset_metadata_too_large = 12,
    // The server disconnected before a response was received.
    network_exception = 13,
    // The coordinator is loading and hence can't process requests.
    coordinator_load_in_progress = 14,
    // The coordinator is not available.
    coordinator_not_available = 15,
    // This is not the correct coordinator.
    not_coordinator = 16,
    // The request attempted to perform an operation on an invalid topic.
    invalid_topic_exception = 17,
    // The request included message batch larger than the configured segment
    // size on the server.
    record_list_too_large = 18,
    // Messages are rejected since there are fewer in-sync replicas than
    // required.
    not_enough_replicas = 19,
    // Messages are written to the log, but to fewer in-sync replicas than
    // required.
    not_enough_replicas_after_append = 20,
    // Produce request specified an invalid value for required acks.
    invalid_required_acks = 21,
    // Specified group generation id is not valid.
    illegal_generation = 22,
    // The group member's supported protocols are incompatible with those of
    // existing members or first group member tried to join with empty protocol
    // type or empty protocol list.
    inconsistent_group_protocol = 23,
    // The configured groupId is invalid.
    invalid_group_id = 24,
    // The coordinator is not aware of this member.
    unknown_member_id = 25,
    // The session timeout is not within the range allowed by the broker (as
    // configured by group.min.session.timeout.ms and
    // group.max.session.timeout.ms).
    invalid_session_timeout = 26,
    // The group is rebalancing, so a rejoin is needed.
    rebalance_in_progress = 27,
    // The committing offset data size is not valid.
    invalid_commit_offset_size = 28,
    // Not authorized to access topics: [Topic authorization failed.]
    topic_authorization_failed = 29,
    // Not authorized to access group: Group authorization failed.
    group_authorization_failed = 30,
    // Cluster authorization failed.
    cluster_authorization_failed = 31,
    // The timestamp of the message is out of acceptable range.
    invalid_timestamp = 32,
    // The broker does not support the requested SASL mechanism.
    unsupported_sasl_mechanism = 33,
    // Request is not valid given the current SASL state.
    illegal_sasl_state = 34,
    // The version of API is not supported.
    unsupported_version = 35,
    // Topic with this name already exists.
    topic_already_exists = 36,
    // Number of partitions is below 1.
    invalid_partitions = 37,
    // Replication factor is below 1 or larger than the number of available
    // brokers.
    invalid_replication_factor = 38,
    // Replica assignment is invalid.
    invalid_replica_assignment = 39,
    // Configuration is invalid.
    invalid_config = 40,
    // This is not the correct controller for this cluster.
    not_controller = 41,
    // This most likely occurs because of a request being malformed by the
    // client library or the message was sent to an incompatible broker. See the
    // broker logs for more details.
    invalid_request = 42,
    // The message format version on the broker does not support the request.
    unsupported_for_message_format = 43,
    // Request parameters do not satisfy the configured policy.
    policy_violation = 44,
    // The broker received an out of order sequence number.
    out_of_order_sequence_number = 45,
    // The broker received a duplicate sequence number.
    duplicate_sequence_number = 46,
    // Producer attempted an operation with an old epoch. Either there is a
    // newer producer with the same transactionalId, or the producer's
    // transaction has been expired by the broker.
    invalid_producer_epoch = 47,
    // The producer attempted a transactional operation in an invalid state.
    invalid_txn_state = 48,
    // The producer attempted to use a producer id which is not currently
    // assigned to its transactional id.
    invalid_producer_id_mapping = 49,
    // The transaction timeout is larger than the maximum value allowed by the
    // broker (as configured by transaction.max.timeout.ms).
    invalid_transaction_timeout = 50,
    // The producer attempted to update a transaction while another concurrent
    // operation on the same transaction was ongoing.
    concurrent_transactions = 51,
    // Indicates that the transaction coordinator sending a WriteTxnMarker is no
    // longer the current coordinator for a given producer.
    transaction_coordinator_fenced = 52,
    // Transactional Id authorization failed.
    transactional_id_authorization_failed = 53,
    // Security features are disabled.
    security_disabled = 54,
    // The broker did not attempt to execute this operation. This may happen for
    // batched RPCs where some operations in the batch failed, causing the
    // broker to respond without trying the rest.
    operation_not_attempted = 55,
    // Disk error when trying to access log file on the disk.
    kafka_storage_error = 56,
    // The user-specified log directory is not found in the broker config.
    log_dir_not_found = 57,
    // SASL Authentication failed.
    sasl_authentication_failed = 58,
    // This exception is raised by the broker if it could not locate the
    // producer metadata associated with the producerId in question. This could
    // happen if, for instance, the producer's records were deleted because
    // their retention time had elapsed. Once the last records of the producerId
    // are removed, the producer's metadata is removed from the broker, and
    // future appends by the producer will return this exception.
    unknown_producer_id = 59,
    // A partition reassignment is in progress.
    reassignment_in_progress = 60,
    // Delegation Token feature is not enabled.
    delegation_token_auth_disabled = 61,
    // Delegation Token is not found on server.
    delegation_token_not_found = 62,
    // Specified Principal is not valid Owner/Renewer.
    delegation_token_owner_mismatch = 63,
    // Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels
    // and on delegation token authenticated channels.
    delegation_token_request_not_allowed = 64,
    // Delegation Token authorization failed.
    delegation_token_authorization_failed = 65,
    // Delegation Token is expired.
    delegation_token_expired = 66,
    // Supplied principalType is not supported.
    invalid_principal_type = 67,
    // The group is not empty.
    non_empty_group = 68,
    // The group id does not exist.
    group_id_not_found = 69,
    // The fetch session ID was not found.
    fetch_session_id_not_found = 70,
    // The fetch session epoch is invalid.
    invalid_fetch_session_epoch = 71,
    // There is no listener on the leader broker that matches the listener on
    // which metadata request was processed.
    listener_not_found = 72,
    // Topic deletion is disabled.
    topic_deletion_disabled = 73,
    // The leader epoch in the request is older than the epoch on the broker.
    fenced_leader_epoch = 74,
    // The leader epoch in the request is newer than the epoch on the broker.
    unknown_leader_epoch = 75,
    // The requesting client does not support the compression type of given
    // partition.
    unsupported_compression_type = 76,
    // Broker epoch has changed.
    stale_broker_epoch = 77,
    // The leader high watermark has not caught up from a recent leader election
    // so the offsets cannot be guaranteed to be monotonically increasing.
    offset_not_available = 78,
    // The group member needs to have a valid member id before actually entering
    // a consumer group.
    member_id_required = 79,
    // The preferred leader was not available.
    preferred_leader_not_available = 80,
    // Consumer group The consumer group has reached its max size. already has
    // the configured maximum number of members.
    group_max_size_reached = 81,
    // No partition reassignment is in progress
    no_reassignment_in_progress = 85,
    // The broker rejected this static consumer since another consumer with the
    // same group.instance.id has registered with a different member.id.
    fenced_instance_id = 82,
    // The consumer group is actively subscribed to the topic
    group_subscribed_to_topic = 86,
    // This record has failed the validation on broker and hence be rejected.
    invalid_record = 87,
    // There are unstable offsets that need to be cleared.
    unstable_offset_commit = 88,
    // Broker declined to process request due to exceeded resource quotas.
    throttling_quota_exceeded = 89,
    // The transactional_id could not be found for describe tx request.
    transactional_id_not_found = 105,
};

std::ostream& operator<<(std::ostream&, error_code);
std::string_view error_code_to_str(error_code error);
std::error_code make_error_code(error_code);

} // namespace kafka

namespace std {

template<>
struct is_error_code_enum<kafka::error_code> : true_type {};

} // namespace std
