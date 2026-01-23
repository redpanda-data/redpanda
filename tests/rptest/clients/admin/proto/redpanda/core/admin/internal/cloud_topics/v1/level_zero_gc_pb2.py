"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(_runtime_version.Domain.PUBLIC, 5, 29, 0, '', 'proto/redpanda/core/admin/internal/cloud_topics/v1/level_zero_gc.proto')
_sym_db = _symbol_database.Default()
from ........proto.redpanda.core.pbgen import options_pb2 as proto_dot_redpanda_dot_core_dot_pbgen_dot_options__pb2
from ........proto.redpanda.core.pbgen import rpc_pb2 as proto_dot_redpanda_dot_core_dot_pbgen_dot_rpc__pb2
from ........proto.redpanda.core.common.v1 import ntp_pb2 as proto_dot_redpanda_dot_core_dot_common_dot_v1_dot_ntp__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\nFproto/redpanda/core/admin/internal/cloud_topics/v1/level_zero_gc.proto\x12,redpanda.core.admin.internal.cloud_topics.v1\x1a\'proto/redpanda/core/pbgen/options.proto\x1a#proto/redpanda/core/pbgen/rpc.proto\x1a\'proto/redpanda/core/common/v1/ntp.proto"\x0e\n\x0cStartRequest"S\n\rStartResponse\x12B\n\x05state\x18\x01 \x01(\x0e23.redpanda.core.admin.internal.cloud_topics.v1.State"\x0e\n\x0cPauseRequest"S\n\rPauseResponse\x12B\n\x05state\x18\x01 \x01(\x0e23.redpanda.core.admin.internal.cloud_topics.v1.State"l\n\x13AdvanceEpochRequest\x12U\n\npartitions\x18\x01 \x03(\x0b2A.redpanda.core.admin.internal.cloud_topics.v1.TopicPartitionEpoch"z\n\x14AdvanceEpochResponse\x12b\n\npartitions\x18\x01 \x03(\x0b2N.redpanda.core.admin.internal.cloud_topics.v1.TopicPartitionAdvanceEpochResult"`\n\x13TopicPartitionEpoch\x12:\n\tpartition\x18\x01 \x01(\x0b2\'.redpanda.core.common.v1.TopicPartition\x12\r\n\x05epoch\x18\x02 \x01(\x03"\xbf\x01\n TopicPartitionAdvanceEpochResult\x12:\n\tpartition\x18\x01 \x01(\x0b2\'.redpanda.core.common.v1.TopicPartition\x12\x0f\n\x05epoch\x18\x02 \x01(\x03H\x00\x12D\n\x05error\x18\x03 \x01(\x0e23.redpanda.core.admin.internal.cloud_topics.v1.ErrorH\x00B\x08\n\x06result"N\n\x0fGetEpochRequest\x12;\n\npartitions\x18\x01 \x03(\x0b2\'.redpanda.core.common.v1.TopicPartition"r\n\x10GetEpochResponse\x12^\n\npartitions\x18\x01 \x03(\x0b2J.redpanda.core.admin.internal.cloud_topics.v1.TopicPartitionGetEpochResult"\xbb\x01\n\x1cTopicPartitionGetEpochResult\x12:\n\tpartition\x18\x01 \x01(\x0b2\'.redpanda.core.common.v1.TopicPartition\x12\x0f\n\x05epoch\x18\x02 \x01(\x03H\x00\x12D\n\x05error\x18\x03 \x01(\x0e23.redpanda.core.admin.internal.cloud_topics.v1.ErrorH\x00B\x08\n\x06result*Q\n\x05State\x12\x17\n\x13L0_GC_STATE_STARTED\x10\x00\x12\x16\n\x12L0_GC_STATE_PAUSED\x10\x01\x12\x17\n\x13L0_GC_STATE_STOPPED\x10\x02*\x9d\x01\n\x05Error\x12\x17\n\x13L0_GC_ERROR_SUCCESS\x10\x00\x12\x16\n\x12L0_GC_ERROR_FAILED\x10\x01\x12\x1f\n\x1bL0_GC_ERROR_TOPIC_NOT_FOUND\x10\x02\x12!\n\x1dL0_GC_ERROR_INVALID_PARTITION\x10\x03\x12\x1f\n\x1bL0_GC_ERROR_NOT_CLOUD_TOPIC\x10\x042\xde\x04\n\x12LevelZeroGcService\x12\x88\x01\n\x05Start\x12:.redpanda.core.admin.internal.cloud_topics.v1.StartRequest\x1a;.redpanda.core.admin.internal.cloud_topics.v1.StartResponse"\x06\xea\x92\x19\x02\x10\x03\x12\x88\x01\n\x05Pause\x12:.redpanda.core.admin.internal.cloud_topics.v1.PauseRequest\x1a;.redpanda.core.admin.internal.cloud_topics.v1.PauseResponse"\x06\xea\x92\x19\x02\x10\x03\x12\x9d\x01\n\x0cAdvanceEpoch\x12A.redpanda.core.admin.internal.cloud_topics.v1.AdvanceEpochRequest\x1aB.redpanda.core.admin.internal.cloud_topics.v1.AdvanceEpochResponse"\x06\xea\x92\x19\x02\x10\x03\x12\x91\x01\n\x08GetEpoch\x12=.redpanda.core.admin.internal.cloud_topics.v1.GetEpochRequest\x1a>.redpanda.core.admin.internal.cloud_topics.v1.GetEpochResponse"\x06\xea\x92\x19\x02\x10\x03B\x1f\xea\x92\x19\x1bproto::admin::level_zero_gcb\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    _globals['DESCRIPTOR']._loaded_options = None
    _globals['DESCRIPTOR']._serialized_options = b'\xea\x92\x19\x1bproto::admin::level_zero_gc'
    _globals['_LEVELZEROGCSERVICE'].methods_by_name['Start']._loaded_options = None
    _globals['_LEVELZEROGCSERVICE'].methods_by_name['Start']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_LEVELZEROGCSERVICE'].methods_by_name['Pause']._loaded_options = None
    _globals['_LEVELZEROGCSERVICE'].methods_by_name['Pause']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_LEVELZEROGCSERVICE'].methods_by_name['AdvanceEpoch']._loaded_options = None
    _globals['_LEVELZEROGCSERVICE'].methods_by_name['AdvanceEpoch']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_LEVELZEROGCSERVICE'].methods_by_name['GetEpoch']._loaded_options = None
    _globals['_LEVELZEROGCSERVICE'].methods_by_name['GetEpoch']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_STATE']._serialized_start = 1353
    _globals['_STATE']._serialized_end = 1434
    _globals['_ERROR']._serialized_start = 1437
    _globals['_ERROR']._serialized_end = 1594
    _globals['_STARTREQUEST']._serialized_start = 239
    _globals['_STARTREQUEST']._serialized_end = 253
    _globals['_STARTRESPONSE']._serialized_start = 255
    _globals['_STARTRESPONSE']._serialized_end = 338
    _globals['_PAUSEREQUEST']._serialized_start = 340
    _globals['_PAUSEREQUEST']._serialized_end = 354
    _globals['_PAUSERESPONSE']._serialized_start = 356
    _globals['_PAUSERESPONSE']._serialized_end = 439
    _globals['_ADVANCEEPOCHREQUEST']._serialized_start = 441
    _globals['_ADVANCEEPOCHREQUEST']._serialized_end = 549
    _globals['_ADVANCEEPOCHRESPONSE']._serialized_start = 551
    _globals['_ADVANCEEPOCHRESPONSE']._serialized_end = 673
    _globals['_TOPICPARTITIONEPOCH']._serialized_start = 675
    _globals['_TOPICPARTITIONEPOCH']._serialized_end = 771
    _globals['_TOPICPARTITIONADVANCEEPOCHRESULT']._serialized_start = 774
    _globals['_TOPICPARTITIONADVANCEEPOCHRESULT']._serialized_end = 965
    _globals['_GETEPOCHREQUEST']._serialized_start = 967
    _globals['_GETEPOCHREQUEST']._serialized_end = 1045
    _globals['_GETEPOCHRESPONSE']._serialized_start = 1047
    _globals['_GETEPOCHRESPONSE']._serialized_end = 1161
    _globals['_TOPICPARTITIONGETEPOCHRESULT']._serialized_start = 1164
    _globals['_TOPICPARTITIONGETEPOCHRESULT']._serialized_end = 1351
    _globals['_LEVELZEROGCSERVICE']._serialized_start = 1597
    _globals['_LEVELZEROGCSERVICE']._serialized_end = 2203