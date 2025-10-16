"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(_runtime_version.Domain.PUBLIC, 5, 29, 0, '', 'proto/redpanda/core/admin/v2/kafka_connections.proto')
_sym_db = _symbol_database.Default()
from ......google.api import field_info_pb2 as google_dot_api_dot_field__info__pb2
from ......proto.redpanda.core.pbgen import options_pb2 as proto_dot_redpanda_dot_core_dot_pbgen_dot_options__pb2
from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2
from google.protobuf import duration_pb2 as google_dot_protobuf_dot_duration__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n4proto/redpanda/core/admin/v2/kafka_connections.proto\x12\x16redpanda.core.admin.v2\x1a\x1bgoogle/api/field_info.proto\x1a\'proto/redpanda/core/pbgen/options.proto\x1a\x1fgoogle/protobuf/timestamp.proto\x1a\x1egoogle/protobuf/duration.proto"\xf3\x07\n\x0fKafkaConnection\x12\x0f\n\x07node_id\x18\x01 \x01(\x05\x12\x10\n\x08shard_id\x18\x02 \x01(\r\x12\x15\n\x03uid\x18\x03 \x01(\tB\x08\xe2\x8c\xcf\xd7\x08\x02\x08\x01\x12;\n\x05state\x18\x04 \x01(\x0e2,.redpanda.core.admin.v2.KafkaConnectionState\x12-\n\topen_time\x18\x05 \x01(\x0b2\x1a.google.protobuf.Timestamp\x12.\n\nclose_time\x18\x06 \x01(\x0b2\x1a.google.protobuf.Timestamp\x12G\n\x13authentication_info\x18\x07 \x01(\x0b2*.redpanda.core.admin.v2.AuthenticationInfo\x12\x15\n\rlistener_name\x18\x08 \x01(\t\x121\n\x08tls_info\x18\t \x01(\x0b2\x1f.redpanda.core.admin.v2.TLSInfo\x12.\n\x06source\x18\n \x01(\x0b2\x1e.redpanda.core.admin.v2.Source\x12\x11\n\tclient_id\x18\x0b \x01(\t\x12\x1c\n\x14client_software_name\x18\x0c \x01(\t\x12\x1f\n\x17client_software_version\x18\r \x01(\t\x12\x18\n\x10transactional_id\x18\x0e \x01(\t\x12\x10\n\x08group_id\x18\x0f \x01(\t\x12\x19\n\x11group_instance_id\x18\x10 \x01(\t\x12\x17\n\x0fgroup_member_id\x18\x11 \x01(\t\x12N\n\x0capi_versions\x18\x12 \x03(\x0b28.redpanda.core.admin.v2.KafkaConnection.ApiVersionsEntry\x120\n\ridle_duration\x18\x13 \x01(\x0b2\x19.google.protobuf.Duration\x12D\n\x12in_flight_requests\x18\x14 \x01(\x0b2(.redpanda.core.admin.v2.InFlightRequests\x12K\n\x18total_request_statistics\x18\x15 \x01(\x0b2).redpanda.core.admin.v2.RequestStatistics\x12L\n\x19recent_request_statistics\x18\x16 \x01(\x0b2).redpanda.core.admin.v2.RequestStatistics\x1a2\n\x10ApiVersionsEntry\x12\x0b\n\x03key\x18\x01 \x01(\x05\x12\r\n\x05value\x18\x02 \x01(\x05:\x028\x01"\xac\x01\n\x12AuthenticationInfo\x12:\n\x05state\x18\x01 \x01(\x0e2+.redpanda.core.admin.v2.AuthenticationState\x12B\n\tmechanism\x18\x02 \x01(\x0e2/.redpanda.core.admin.v2.AuthenticationMechanism\x12\x16\n\x0euser_principal\x18\x03 \x01(\t"\x1a\n\x07TLSInfo\x12\x0f\n\x07enabled\x18\x01 \x01(\x08"4\n\x06Source\x12\x1c\n\nip_address\x18\x01 \x01(\tB\x08\xe2\x8c\xcf\xd7\x08\x02\x08\x04\x12\x0c\n\x04port\x18\x02 \x01(\r"\xd6\x01\n\x10InFlightRequests\x12T\n\x1asampled_in_flight_requests\x18\x01 \x03(\x0b20.redpanda.core.admin.v2.InFlightRequests.Request\x12\x19\n\x11has_more_requests\x18\x02 \x01(\x08\x1aQ\n\x07Request\x12\x0f\n\x07api_key\x18\x01 \x01(\x05\x125\n\x12in_flight_duration\x18\x02 \x01(\x0b2\x19.google.protobuf.Duration"s\n\x11RequestStatistics\x12\x15\n\rproduce_bytes\x18\x01 \x01(\x04\x12\x13\n\x0bfetch_bytes\x18\x02 \x01(\x04\x12\x15\n\rrequest_count\x18\x03 \x01(\x04\x12\x1b\n\x13produce_batch_count\x18\x04 \x01(\x04*\xa7\x01\n\x14KafkaConnectionState\x12&\n"KAFKA_CONNECTION_STATE_UNSPECIFIED\x10\x00\x12\x1f\n\x1bKAFKA_CONNECTION_STATE_OPEN\x10\x01\x12#\n\x1fKAFKA_CONNECTION_STATE_ABORTING\x10\x02\x12!\n\x1dKAFKA_CONNECTION_STATE_CLOSED\x10\x03*\xa9\x01\n\x13AuthenticationState\x12$\n AUTHENTICATION_STATE_UNSPECIFIED\x10\x00\x12(\n$AUTHENTICATION_STATE_UNAUTHENTICATED\x10\x01\x12 \n\x1cAUTHENTICATION_STATE_SUCCESS\x10\x02\x12 \n\x1cAUTHENTICATION_STATE_FAILURE\x10\x03*\x91\x02\n\x17AuthenticationMechanism\x12(\n$AUTHENTICATION_MECHANISM_UNSPECIFIED\x10\x00\x12!\n\x1dAUTHENTICATION_MECHANISM_MTLS\x10\x01\x12\'\n#AUTHENTICATION_MECHANISM_SASL_SCRAM\x10\x02\x12-\n)AUTHENTICATION_MECHANISM_SASL_OAUTHBEARER\x10\x03\x12\'\n#AUTHENTICATION_MECHANISM_SASL_PLAIN\x10\x04\x12(\n$AUTHENTICATION_MECHANISM_SASL_GSSAPI\x10\x05B\x10\xea\x92\x19\x0cproto::adminb\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'proto.redpanda.core.admin.v2.kafka_connections_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    _globals['DESCRIPTOR']._loaded_options = None
    _globals['DESCRIPTOR']._serialized_options = b'\xea\x92\x19\x0cproto::admin'
    _globals['_KAFKACONNECTION_APIVERSIONSENTRY']._loaded_options = None
    _globals['_KAFKACONNECTION_APIVERSIONSENTRY']._serialized_options = b'8\x01'
    _globals['_KAFKACONNECTION'].fields_by_name['uid']._loaded_options = None
    _globals['_KAFKACONNECTION'].fields_by_name['uid']._serialized_options = b'\xe2\x8c\xcf\xd7\x08\x02\x08\x01'
    _globals['_SOURCE'].fields_by_name['ip_address']._loaded_options = None
    _globals['_SOURCE'].fields_by_name['ip_address']._serialized_options = b'\xe2\x8c\xcf\xd7\x08\x02\x08\x04'
    _globals['_KAFKACONNECTIONSTATE']._serialized_start = 1821
    _globals['_KAFKACONNECTIONSTATE']._serialized_end = 1988
    _globals['_AUTHENTICATIONSTATE']._serialized_start = 1991
    _globals['_AUTHENTICATIONSTATE']._serialized_end = 2160
    _globals['_AUTHENTICATIONMECHANISM']._serialized_start = 2163
    _globals['_AUTHENTICATIONMECHANISM']._serialized_end = 2436
    _globals['_KAFKACONNECTION']._serialized_start = 216
    _globals['_KAFKACONNECTION']._serialized_end = 1227
    _globals['_KAFKACONNECTION_APIVERSIONSENTRY']._serialized_start = 1177
    _globals['_KAFKACONNECTION_APIVERSIONSENTRY']._serialized_end = 1227
    _globals['_AUTHENTICATIONINFO']._serialized_start = 1230
    _globals['_AUTHENTICATIONINFO']._serialized_end = 1402
    _globals['_TLSINFO']._serialized_start = 1404
    _globals['_TLSINFO']._serialized_end = 1430
    _globals['_SOURCE']._serialized_start = 1432
    _globals['_SOURCE']._serialized_end = 1484
    _globals['_INFLIGHTREQUESTS']._serialized_start = 1487
    _globals['_INFLIGHTREQUESTS']._serialized_end = 1701
    _globals['_INFLIGHTREQUESTS_REQUEST']._serialized_start = 1620
    _globals['_INFLIGHTREQUESTS_REQUEST']._serialized_end = 1701
    _globals['_REQUESTSTATISTICS']._serialized_start = 1703
    _globals['_REQUESTSTATISTICS']._serialized_end = 1818