"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(_runtime_version.Domain.PUBLIC, 5, 29, 0, '', 'proto/redpanda/core/admin/v2/plugin.proto')
_sym_db = _symbol_database.Default()
from ......proto.redpanda.core.common.v1 import compression_pb2 as proto_dot_redpanda_dot_core_dot_common_dot_v1_dot_compression__pb2
from ......proto.redpanda.core.pbgen import options_pb2 as proto_dot_redpanda_dot_core_dot_pbgen_dot_options__pb2
from ......proto.redpanda.core.pbgen import rpc_pb2 as proto_dot_redpanda_dot_core_dot_pbgen_dot_rpc__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n)proto/redpanda/core/admin/v2/plugin.proto\x12\x16redpanda.core.admin.v2\x1a/proto/redpanda/core/common/v1/compression.proto\x1a\'proto/redpanda/core/pbgen/options.proto\x1a#proto/redpanda/core/pbgen/rpc.proto"\xa4\x02\n\tTransform\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x13\n\x0binput_topic\x18\x02 \x01(\t\x12\x15\n\routput_topics\x18\x03 \x03(\t\x12I\n\x0benvironment\x18\x04 \x03(\x0b24.redpanda.core.admin.v2.TransformEnvironmentVariable\x12=\n\x0bcompression\x18\x05 \x01(\x0e2(.redpanda.core.common.v1.CompressionMode\x12\x11\n\tis_paused\x18\x06 \x01(\x08\x12@\n\x06status\x18\x07 \x03(\x0b20.redpanda.core.admin.v2.TransformPartitionStatus":\n\x1cTransformEnvironmentVariable\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t"\x8c\x01\n\x18TransformPartitionStatus\x12\x0f\n\x07node_id\x18\x01 \x01(\x05\x12\x11\n\tpartition\x18\x02 \x01(\x05\x12?\n\x06status\x18\x03 \x01(\x0e2/.redpanda.core.admin.v2.TransformPartitionState\x12\x0b\n\x03lag\x18\x04 \x01(\x03"f\n\x16TransformConsumeOffset\x12\x14\n\nfrom_start\x18\x01 \x01(\x03H\x00\x12\x12\n\x08from_end\x18\x02 \x01(\x03H\x00\x12\x16\n\x0ctimestamp_ms\x18\x03 \x01(\x03H\x00B\n\n\x08position"\xb7\x02\n\x16CreateTransformRequest\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x13\n\x0binput_topic\x18\x02 \x01(\t\x12\x15\n\routput_topics\x18\x03 \x03(\t\x12I\n\x0benvironment\x18\x04 \x03(\x0b24.redpanda.core.admin.v2.TransformEnvironmentVariable\x12=\n\x0bcompression\x18\x05 \x01(\x0e2(.redpanda.core.common.v1.CompressionMode\x12F\n\x0econsume_offset\x18\x06 \x01(\x0b2..redpanda.core.admin.v2.TransformConsumeOffset\x12\x11\n\tbinary_id\x18\x07 \x01(\t"\x19\n\x17CreateTransformResponse"#\n\x13GetTransformRequest\x12\x0c\n\x04name\x18\x01 \x01(\t"L\n\x14GetTransformResponse\x124\n\ttransform\x18\x01 \x01(\x0b2!.redpanda.core.admin.v2.Transform"\x17\n\x15ListTransformsRequest"O\n\x16ListTransformsResponse\x125\n\ntransforms\x18\x01 \x03(\x0b2!.redpanda.core.admin.v2.Transform"\x8c\x02\n\x16UpdateTransformRequest\x12\x0c\n\x04name\x18\x01 \x01(\t\x12I\n\x0benvironment\x18\x02 \x03(\x0b24.redpanda.core.admin.v2.TransformEnvironmentVariable\x12\x17\n\x0fhas_environment\x18\x03 \x01(\x08\x12\x11\n\tis_paused\x18\x04 \x01(\x08\x12\x15\n\rhas_is_paused\x18\x05 \x01(\x08\x12=\n\x0bcompression\x18\x06 \x01(\x0e2(.redpanda.core.common.v1.CompressionMode\x12\x17\n\x0fhas_compression\x18\x07 \x01(\x08"\x19\n\x17UpdateTransformResponse"&\n\x16DeleteTransformRequest\x12\x0c\n\x04name\x18\x01 \x01(\t"\x19\n\x17DeleteTransformResponse";\n\x0fTransformBinary\x12\x11\n\tbinary_id\x18\x01 \x01(\t\x12\x15\n\rreferenced_by\x18\x02 \x03(\t"%\n\x13CreateBinaryRequest\x12\x0e\n\x06binary\x18\x01 \x01(\x0c")\n\x14CreateBinaryResponse\x12\x11\n\tbinary_id\x18\x01 \x01(\t"\x15\n\x13ListBinariesRequest"Q\n\x14ListBinariesResponse\x129\n\x08binaries\x18\x01 \x03(\x0b2\'.redpanda.core.admin.v2.TransformBinary"(\n\x13DeleteBinaryRequest\x12\x11\n\tbinary_id\x18\x01 \x01(\t"\x16\n\x14DeleteBinaryResponse*\xe1\x01\n\x17TransformPartitionState\x12)\n%TRANSFORM_PARTITION_STATE_UNSPECIFIED\x10\x00\x12%\n!TRANSFORM_PARTITION_STATE_RUNNING\x10\x01\x12&\n"TRANSFORM_PARTITION_STATE_INACTIVE\x10\x02\x12%\n!TRANSFORM_PARTITION_STATE_ERRORED\x10\x03\x12%\n!TRANSFORM_PARTITION_STATE_UNKNOWN\x10\x042\xc8\x07\n\rPluginService\x12z\n\x0fCreateTransform\x12..redpanda.core.admin.v2.CreateTransformRequest\x1a/.redpanda.core.admin.v2.CreateTransformResponse"\x06\xea\x92\x19\x02\x10\x03\x12q\n\x0cGetTransform\x12+.redpanda.core.admin.v2.GetTransformRequest\x1a,.redpanda.core.admin.v2.GetTransformResponse"\x06\xea\x92\x19\x02\x10\x02\x12w\n\x0eListTransforms\x12-.redpanda.core.admin.v2.ListTransformsRequest\x1a..redpanda.core.admin.v2.ListTransformsResponse"\x06\xea\x92\x19\x02\x10\x02\x12z\n\x0fUpdateTransform\x12..redpanda.core.admin.v2.UpdateTransformRequest\x1a/.redpanda.core.admin.v2.UpdateTransformResponse"\x06\xea\x92\x19\x02\x10\x03\x12z\n\x0fDeleteTransform\x12..redpanda.core.admin.v2.DeleteTransformRequest\x1a/.redpanda.core.admin.v2.DeleteTransformResponse"\x06\xea\x92\x19\x02\x10\x03\x12q\n\x0cCreateBinary\x12+.redpanda.core.admin.v2.CreateBinaryRequest\x1a,.redpanda.core.admin.v2.CreateBinaryResponse"\x06\xea\x92\x19\x02\x10\x03\x12q\n\x0cListBinaries\x12+.redpanda.core.admin.v2.ListBinariesRequest\x1a,.redpanda.core.admin.v2.ListBinariesResponse"\x06\xea\x92\x19\x02\x10\x02\x12q\n\x0cDeleteBinary\x12+.redpanda.core.admin.v2.DeleteBinaryRequest\x1a,.redpanda.core.admin.v2.DeleteBinaryResponse"\x06\xea\x92\x19\x02\x10\x03B\x10\xea\x92\x19\x0cproto::adminb\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'proto.redpanda.core.admin.v2.plugin_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    _globals['DESCRIPTOR']._loaded_options = None
    _globals['DESCRIPTOR']._serialized_options = b'\xea\x92\x19\x0cproto::admin'
    _globals['_PLUGINSERVICE'].methods_by_name['CreateTransform']._loaded_options = None
    _globals['_PLUGINSERVICE'].methods_by_name['CreateTransform']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_PLUGINSERVICE'].methods_by_name['GetTransform']._loaded_options = None
    _globals['_PLUGINSERVICE'].methods_by_name['GetTransform']._serialized_options = b'\xea\x92\x19\x02\x10\x02'
    _globals['_PLUGINSERVICE'].methods_by_name['ListTransforms']._loaded_options = None
    _globals['_PLUGINSERVICE'].methods_by_name['ListTransforms']._serialized_options = b'\xea\x92\x19\x02\x10\x02'
    _globals['_PLUGINSERVICE'].methods_by_name['UpdateTransform']._loaded_options = None
    _globals['_PLUGINSERVICE'].methods_by_name['UpdateTransform']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_PLUGINSERVICE'].methods_by_name['DeleteTransform']._loaded_options = None
    _globals['_PLUGINSERVICE'].methods_by_name['DeleteTransform']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_PLUGINSERVICE'].methods_by_name['CreateBinary']._loaded_options = None
    _globals['_PLUGINSERVICE'].methods_by_name['CreateBinary']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_PLUGINSERVICE'].methods_by_name['ListBinaries']._loaded_options = None
    _globals['_PLUGINSERVICE'].methods_by_name['ListBinaries']._serialized_options = b'\xea\x92\x19\x02\x10\x02'
    _globals['_PLUGINSERVICE'].methods_by_name['DeleteBinary']._loaded_options = None
    _globals['_PLUGINSERVICE'].methods_by_name['DeleteBinary']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_TRANSFORMPARTITIONSTATE']._serialized_start = 2041
    _globals['_TRANSFORMPARTITIONSTATE']._serialized_end = 2266
    _globals['_TRANSFORM']._serialized_start = 197
    _globals['_TRANSFORM']._serialized_end = 489
    _globals['_TRANSFORMENVIRONMENTVARIABLE']._serialized_start = 491
    _globals['_TRANSFORMENVIRONMENTVARIABLE']._serialized_end = 549
    _globals['_TRANSFORMPARTITIONSTATUS']._serialized_start = 552
    _globals['_TRANSFORMPARTITIONSTATUS']._serialized_end = 692
    _globals['_TRANSFORMCONSUMEOFFSET']._serialized_start = 694
    _globals['_TRANSFORMCONSUMEOFFSET']._serialized_end = 796
    _globals['_CREATETRANSFORMREQUEST']._serialized_start = 799
    _globals['_CREATETRANSFORMREQUEST']._serialized_end = 1110
    _globals['_CREATETRANSFORMRESPONSE']._serialized_start = 1112
    _globals['_CREATETRANSFORMRESPONSE']._serialized_end = 1137
    _globals['_GETTRANSFORMREQUEST']._serialized_start = 1139
    _globals['_GETTRANSFORMREQUEST']._serialized_end = 1174
    _globals['_GETTRANSFORMRESPONSE']._serialized_start = 1176
    _globals['_GETTRANSFORMRESPONSE']._serialized_end = 1252
    _globals['_LISTTRANSFORMSREQUEST']._serialized_start = 1254
    _globals['_LISTTRANSFORMSREQUEST']._serialized_end = 1277
    _globals['_LISTTRANSFORMSRESPONSE']._serialized_start = 1279
    _globals['_LISTTRANSFORMSRESPONSE']._serialized_end = 1358
    _globals['_UPDATETRANSFORMREQUEST']._serialized_start = 1361
    _globals['_UPDATETRANSFORMREQUEST']._serialized_end = 1629
    _globals['_UPDATETRANSFORMRESPONSE']._serialized_start = 1631
    _globals['_UPDATETRANSFORMRESPONSE']._serialized_end = 1656
    _globals['_DELETETRANSFORMREQUEST']._serialized_start = 1658
    _globals['_DELETETRANSFORMREQUEST']._serialized_end = 1696
    _globals['_DELETETRANSFORMRESPONSE']._serialized_start = 1698
    _globals['_DELETETRANSFORMRESPONSE']._serialized_end = 1723
    _globals['_TRANSFORMBINARY']._serialized_start = 1725
    _globals['_TRANSFORMBINARY']._serialized_end = 1784
    _globals['_CREATEBINARYREQUEST']._serialized_start = 1786
    _globals['_CREATEBINARYREQUEST']._serialized_end = 1823
    _globals['_CREATEBINARYRESPONSE']._serialized_start = 1825
    _globals['_CREATEBINARYRESPONSE']._serialized_end = 1866
    _globals['_LISTBINARIESREQUEST']._serialized_start = 1868
    _globals['_LISTBINARIESREQUEST']._serialized_end = 1889
    _globals['_LISTBINARIESRESPONSE']._serialized_start = 1891
    _globals['_LISTBINARIESRESPONSE']._serialized_end = 1972
    _globals['_DELETEBINARYREQUEST']._serialized_start = 1974
    _globals['_DELETEBINARYREQUEST']._serialized_end = 2014
    _globals['_DELETEBINARYRESPONSE']._serialized_start = 2016
    _globals['_DELETEBINARYRESPONSE']._serialized_end = 2038
    _globals['_PLUGINSERVICE']._serialized_start = 2269
    _globals['_PLUGINSERVICE']._serialized_end = 3237