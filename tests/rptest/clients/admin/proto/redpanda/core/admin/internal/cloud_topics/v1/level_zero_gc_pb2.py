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
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\nFproto/redpanda/core/admin/internal/cloud_topics/v1/level_zero_gc.proto\x12,redpanda.core.admin.internal.cloud_topics.v1\x1a\'proto/redpanda/core/pbgen/options.proto\x1a#proto/redpanda/core/pbgen/rpc.proto\x1a\'proto/redpanda/core/common/v1/ntp.proto"4\n\x10GetStatusRequest\x12\x14\n\x07node_id\x18\x01 \x01(\x05H\x00\x88\x01\x01B\n\n\x08_node_id"\\\n\x11GetStatusResponse\x12G\n\x05nodes\x18\x01 \x03(\x0b28.redpanda.core.admin.internal.cloud_topics.v1.NodeStatus"\x86\x01\n\nNodeStatus\x12\x0f\n\x07node_id\x18\x01 \x01(\x05\x12I\n\x06shards\x18\x02 \x03(\x0b29.redpanda.core.admin.internal.cloud_topics.v1.ShardStatus\x12\x12\n\x05error\x18\x03 \x01(\tH\x00\x88\x01\x01B\x08\n\x06_error"e\n\x0bShardStatus\x12\x10\n\x08shard_id\x18\x01 \x01(\x05\x12D\n\x06status\x18\x02 \x01(\x0e24.redpanda.core.admin.internal.cloud_topics.v1.Status"0\n\x0cStartRequest\x12\x14\n\x07node_id\x18\x01 \x01(\x05H\x00\x88\x01\x01B\n\n\x08_node_id"[\n\rStartResponse\x12J\n\x07results\x18\x01 \x03(\x0b29.redpanda.core.admin.internal.cloud_topics.v1.StartResult"0\n\x0cPauseRequest\x12\x14\n\x07node_id\x18\x01 \x01(\x05H\x00\x88\x01\x01B\n\n\x08_node_id"[\n\rPauseResponse\x12J\n\x07results\x18\x01 \x03(\x0b29.redpanda.core.admin.internal.cloud_topics.v1.PauseResult"<\n\x0bStartResult\x12\x0f\n\x07node_id\x18\x01 \x01(\x05\x12\x12\n\x05error\x18\x03 \x01(\tH\x00\x88\x01\x01B\x08\n\x06_error"<\n\x0bPauseResult\x12\x0f\n\x07node_id\x18\x01 \x01(\x05\x12\x12\n\x05error\x18\x03 \x01(\tH\x00\x88\x01\x01B\x08\n\x06_error"Q\n\x13AdvanceEpochRequest\x12:\n\tpartition\x18\x01 \x01(\x0b2\'.redpanda.core.common.v1.TopicPartition"{\n\x14AdvanceEpochResponse\x12H\n\x05epoch\x18\x01 \x01(\x0b27.redpanda.core.admin.internal.cloud_topics.v1.EpochInfoH\x00\x12\x0f\n\x05error\x18\x02 \x01(\tH\x00B\x08\n\x06result"R\n\x13GetEpochInfoRequest\x12;\n\npartitions\x18\x01 \x03(\x0b2\'.redpanda.core.common.v1.TopicPartition"\x8b\x01\n\x14GetEpochInfoResponse\x12U\n\x06epochs\x18\x01 \x03(\x0b2E.redpanda.core.admin.internal.cloud_topics.v1.TopicPartitionEpochInfo\x12\x12\n\x05error\x18\x02 \x01(\tH\x00\x88\x01\x01B\x08\n\x06_error"\xbf\x01\n\x17TopicPartitionEpochInfo\x12:\n\tpartition\x18\x01 \x01(\x0b2\'.redpanda.core.common.v1.TopicPartition\x12M\n\nepoch_info\x18\x02 \x01(\x0b27.redpanda.core.admin.internal.cloud_topics.v1.EpochInfoH\x00\x12\x0f\n\x05error\x18\x03 \x01(\tH\x00B\x08\n\x06result"\x91\x01\n\tEpochInfo\x12 \n\x18estimated_inactive_epoch\x18\x01 \x01(\x03\x12\x19\n\x11max_applied_epoch\x18\x02 \x01(\x03\x12"\n\x1alast_reconciled_log_offset\x18\x03 \x01(\x03\x12#\n\x1bcurrent_epoch_window_offset\x18\x04 \x01(\x03*\x8e\x01\n\x06Status\x12\x1c\n\x18L0_GC_STATUS_UNSPECIFIED\x10\x00\x12\x17\n\x13L0_GC_STATUS_PAUSED\x10\x01\x12\x18\n\x14L0_GC_STATUS_RUNNING\x10\x02\x12\x19\n\x15L0_GC_STATUS_STOPPING\x10\x03\x12\x18\n\x14L0_GC_STATUS_STOPPED\x10\x042\x81\x06\n\x12LevelZeroGcService\x12\x94\x01\n\tGetStatus\x12>.redpanda.core.admin.internal.cloud_topics.v1.GetStatusRequest\x1a?.redpanda.core.admin.internal.cloud_topics.v1.GetStatusResponse"\x06\xea\x92\x19\x02\x10\x03\x12\x88\x01\n\x05Start\x12:.redpanda.core.admin.internal.cloud_topics.v1.StartRequest\x1a;.redpanda.core.admin.internal.cloud_topics.v1.StartResponse"\x06\xea\x92\x19\x02\x10\x03\x12\x88\x01\n\x05Pause\x12:.redpanda.core.admin.internal.cloud_topics.v1.PauseRequest\x1a;.redpanda.core.admin.internal.cloud_topics.v1.PauseResponse"\x06\xea\x92\x19\x02\x10\x03\x12\x9d\x01\n\x0cAdvanceEpoch\x12A.redpanda.core.admin.internal.cloud_topics.v1.AdvanceEpochRequest\x1aB.redpanda.core.admin.internal.cloud_topics.v1.AdvanceEpochResponse"\x06\xea\x92\x19\x02\x10\x03\x12\x9d\x01\n\x0cGetEpochInfo\x12A.redpanda.core.admin.internal.cloud_topics.v1.GetEpochInfoRequest\x1aB.redpanda.core.admin.internal.cloud_topics.v1.GetEpochInfoResponse"\x06\xea\x92\x19\x02\x10\x03B\x1f\xea\x92\x19\x1bproto::admin::level_zero_gcb\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    _globals['DESCRIPTOR']._loaded_options = None
    _globals['DESCRIPTOR']._serialized_options = b'\xea\x92\x19\x1bproto::admin::level_zero_gc'
    _globals['_LEVELZEROGCSERVICE'].methods_by_name['GetStatus']._loaded_options = None
    _globals['_LEVELZEROGCSERVICE'].methods_by_name['GetStatus']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_LEVELZEROGCSERVICE'].methods_by_name['Start']._loaded_options = None
    _globals['_LEVELZEROGCSERVICE'].methods_by_name['Start']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_LEVELZEROGCSERVICE'].methods_by_name['Pause']._loaded_options = None
    _globals['_LEVELZEROGCSERVICE'].methods_by_name['Pause']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_LEVELZEROGCSERVICE'].methods_by_name['AdvanceEpoch']._loaded_options = None
    _globals['_LEVELZEROGCSERVICE'].methods_by_name['AdvanceEpoch']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_LEVELZEROGCSERVICE'].methods_by_name['GetEpochInfo']._loaded_options = None
    _globals['_LEVELZEROGCSERVICE'].methods_by_name['GetEpochInfo']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_STATUS']._serialized_start = 1814
    _globals['_STATUS']._serialized_end = 1956
    _globals['_GETSTATUSREQUEST']._serialized_start = 239
    _globals['_GETSTATUSREQUEST']._serialized_end = 291
    _globals['_GETSTATUSRESPONSE']._serialized_start = 293
    _globals['_GETSTATUSRESPONSE']._serialized_end = 385
    _globals['_NODESTATUS']._serialized_start = 388
    _globals['_NODESTATUS']._serialized_end = 522
    _globals['_SHARDSTATUS']._serialized_start = 524
    _globals['_SHARDSTATUS']._serialized_end = 625
    _globals['_STARTREQUEST']._serialized_start = 627
    _globals['_STARTREQUEST']._serialized_end = 675
    _globals['_STARTRESPONSE']._serialized_start = 677
    _globals['_STARTRESPONSE']._serialized_end = 768
    _globals['_PAUSEREQUEST']._serialized_start = 770
    _globals['_PAUSEREQUEST']._serialized_end = 818
    _globals['_PAUSERESPONSE']._serialized_start = 820
    _globals['_PAUSERESPONSE']._serialized_end = 911
    _globals['_STARTRESULT']._serialized_start = 913
    _globals['_STARTRESULT']._serialized_end = 973
    _globals['_PAUSERESULT']._serialized_start = 975
    _globals['_PAUSERESULT']._serialized_end = 1035
    _globals['_ADVANCEEPOCHREQUEST']._serialized_start = 1037
    _globals['_ADVANCEEPOCHREQUEST']._serialized_end = 1118
    _globals['_ADVANCEEPOCHRESPONSE']._serialized_start = 1120
    _globals['_ADVANCEEPOCHRESPONSE']._serialized_end = 1243
    _globals['_GETEPOCHINFOREQUEST']._serialized_start = 1245
    _globals['_GETEPOCHINFOREQUEST']._serialized_end = 1327
    _globals['_GETEPOCHINFORESPONSE']._serialized_start = 1330
    _globals['_GETEPOCHINFORESPONSE']._serialized_end = 1469
    _globals['_TOPICPARTITIONEPOCHINFO']._serialized_start = 1472
    _globals['_TOPICPARTITIONEPOCHINFO']._serialized_end = 1663
    _globals['_EPOCHINFO']._serialized_start = 1666
    _globals['_EPOCHINFO']._serialized_end = 1811
    _globals['_LEVELZEROGCSERVICE']._serialized_start = 1959
    _globals['_LEVELZEROGCSERVICE']._serialized_end = 2728