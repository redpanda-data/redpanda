"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(_runtime_version.Domain.PUBLIC, 5, 29, 0, '', 'proto/redpanda/core/rest/iceberg.proto')
_sym_db = _symbol_database.Default()
from .....proto.redpanda.core.pbgen import options_pb2 as proto_dot_redpanda_dot_core_dot_pbgen_dot_options__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n&proto/redpanda/core/rest/iceberg.proto\x12\x15redpanda.core.rest.v1\x1a\'proto/redpanda/core/pbgen/options.proto"^\n\x0ePartitionState\x12*\n\x1dlast_catalog_committed_offset\x18\x01 \x01(\x03H\x00\x88\x01\x01B \n\x1e_last_catalog_committed_offset"\x8f\x03\n\nTopicState\x12P\n\x10partition_states\x18\x01 \x03(\x0b26.redpanda.core.rest.v1.TopicState.PartitionStatesEntry\x12D\n\x12translation_status\x18\x02 \x01(\x0e2(.redpanda.core.rest.v1.TranslationStatus\x12\'\n\x1alast_committed_snapshot_id\x18\x03 \x01(\x04H\x00\x88\x01\x01\x12\x16\n\x0enamespace_name\x18\x04 \x03(\t\x12\x12\n\ntable_name\x18\x05 \x01(\t\x12\x16\n\x0edlq_table_name\x18\x06 \x01(\t\x1a]\n\x14PartitionStatesEntry\x12\x0b\n\x03key\x18\x01 \x01(\x05\x124\n\x05value\x18\x02 \x01(\x0b2%.redpanda.core.rest.v1.PartitionState:\x028\x01B\x1d\n\x1b_last_committed_snapshot_id"3\n\x1aGetTranslationStateRequest\x12\x15\n\rtopics_filter\x18\x01 \x03(\t"\xcf\x01\n\x1bGetTranslationStateResponse\x12Y\n\x0ctopic_states\x18\x01 \x03(\x0b2C.redpanda.core.rest.v1.GetTranslationStateResponse.TopicStatesEntry\x1aU\n\x10TopicStatesEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x120\n\x05value\x18\x02 \x01(\x0b2!.redpanda.core.rest.v1.TopicState:\x028\x01*x\n\x11TranslationStatus\x12"\n\x1eTRANSLATION_STATUS_UNSPECIFIED\x10\x00\x12\x1f\n\x1bTRANSLATION_STATUS_DISABLED\x10\x01\x12\x1e\n\x1aTRANSLATION_STATUS_ENABLED\x10\x02B\x15\xea\x92\x19\x11proto::pandaproxyb\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'proto.redpanda.core.rest.iceberg_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    _globals['DESCRIPTOR']._loaded_options = None
    _globals['DESCRIPTOR']._serialized_options = b'\xea\x92\x19\x11proto::pandaproxy'
    _globals['_TOPICSTATE_PARTITIONSTATESENTRY']._loaded_options = None
    _globals['_TOPICSTATE_PARTITIONSTATESENTRY']._serialized_options = b'8\x01'
    _globals['_GETTRANSLATIONSTATERESPONSE_TOPICSTATESENTRY']._loaded_options = None
    _globals['_GETTRANSLATIONSTATERESPONSE_TOPICSTATESENTRY']._serialized_options = b'8\x01'
    _globals['_TRANSLATIONSTATUS']._serialized_start = 867
    _globals['_TRANSLATIONSTATUS']._serialized_end = 987
    _globals['_PARTITIONSTATE']._serialized_start = 106
    _globals['_PARTITIONSTATE']._serialized_end = 200
    _globals['_TOPICSTATE']._serialized_start = 203
    _globals['_TOPICSTATE']._serialized_end = 602
    _globals['_TOPICSTATE_PARTITIONSTATESENTRY']._serialized_start = 478
    _globals['_TOPICSTATE_PARTITIONSTATESENTRY']._serialized_end = 571
    _globals['_GETTRANSLATIONSTATEREQUEST']._serialized_start = 604
    _globals['_GETTRANSLATIONSTATEREQUEST']._serialized_end = 655
    _globals['_GETTRANSLATIONSTATERESPONSE']._serialized_start = 658
    _globals['_GETTRANSLATIONSTATERESPONSE']._serialized_end = 865
    _globals['_GETTRANSLATIONSTATERESPONSE_TOPICSTATESENTRY']._serialized_start = 780
    _globals['_GETTRANSLATIONSTATERESPONSE_TOPICSTATESENTRY']._serialized_end = 865