"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(_runtime_version.Domain.PUBLIC, 5, 29, 0, '', 'proto/redpanda/core/common/v1/acl.proto')
_sym_db = _symbol_database.Default()
from ......proto.redpanda.core.pbgen import options_pb2 as proto_dot_redpanda_dot_core_dot_pbgen_dot_options__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\'proto/redpanda/core/common/v1/acl.proto\x12\x17redpanda.core.common.v1\x1a\'proto/redpanda/core/pbgen/options.proto*\xf8\x01\n\x0bACLResource\x12\x1c\n\x18ACL_RESOURCE_UNSPECIFIED\x10\x00\x12\x14\n\x10ACL_RESOURCE_ANY\x10\x01\x12\x18\n\x14ACL_RESOURCE_CLUSTER\x10\x02\x12\x16\n\x12ACL_RESOURCE_GROUP\x10\x03\x12\x16\n\x12ACL_RESOURCE_TOPIC\x10\x04\x12\x17\n\x13ACL_RESOURCE_TXN_ID\x10\x05\x12\x1b\n\x17ACL_RESOURCE_SR_SUBJECT\x10\x06\x12\x1c\n\x18ACL_RESOURCE_SR_REGISTRY\x10\x07\x12\x17\n\x13ACL_RESOURCE_SR_ANY\x10\x08*\xa4\x01\n\nACLPattern\x12\x1b\n\x17ACL_PATTERN_UNSPECIFIED\x10\x00\x12\x13\n\x0fACL_PATTERN_ANY\x10\x01\x12\x17\n\x13ACL_PATTERN_LITERAL\x10\x02\x12\x18\n\x14ACL_PATTERN_PREFIXED\x10\x03\x12\x16\n\x12ACL_PATTERN_PREFIX\x10\x03\x12\x15\n\x11ACL_PATTERN_MATCH\x10\x04\x1a\x02\x10\x01*\xe9\x02\n\x0cACLOperation\x12\x1d\n\x19ACL_OPERATION_UNSPECIFIED\x10\x00\x12\x15\n\x11ACL_OPERATION_ANY\x10\x01\x12\x16\n\x12ACL_OPERATION_READ\x10\x02\x12\x17\n\x13ACL_OPERATION_WRITE\x10\x03\x12\x18\n\x14ACL_OPERATION_CREATE\x10\x04\x12\x18\n\x14ACL_OPERATION_REMOVE\x10\x05\x12\x17\n\x13ACL_OPERATION_ALTER\x10\x06\x12\x1a\n\x16ACL_OPERATION_DESCRIBE\x10\x07\x12 \n\x1cACL_OPERATION_CLUSTER_ACTION\x10\x08\x12"\n\x1eACL_OPERATION_DESCRIBE_CONFIGS\x10\t\x12\x1f\n\x1bACL_OPERATION_ALTER_CONFIGS\x10\n\x12"\n\x1eACL_OPERATION_IDEMPOTENT_WRITE\x10\x0b*\x92\x01\n\x11ACLPermissionType\x12#\n\x1fACL_PERMISSION_TYPE_UNSPECIFIED\x10\x00\x12\x1b\n\x17ACL_PERMISSION_TYPE_ANY\x10\x01\x12\x1d\n\x19ACL_PERMISSION_TYPE_ALLOW\x10\x02\x12\x1c\n\x18ACL_PERMISSION_TYPE_DENY\x10\x03B\x11\xea\x92\x19\rproto::commonb\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'proto.redpanda.core.common.v1.acl_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    _globals['DESCRIPTOR']._loaded_options = None
    _globals['DESCRIPTOR']._serialized_options = b'\xea\x92\x19\rproto::common'
    _globals['_ACLPATTERN']._loaded_options = None
    _globals['_ACLPATTERN']._serialized_options = b'\x10\x01'
    _globals['_ACLRESOURCE']._serialized_start = 110
    _globals['_ACLRESOURCE']._serialized_end = 358
    _globals['_ACLPATTERN']._serialized_start = 361
    _globals['_ACLPATTERN']._serialized_end = 525
    _globals['_ACLOPERATION']._serialized_start = 528
    _globals['_ACLOPERATION']._serialized_end = 889
    _globals['_ACLPERMISSIONTYPE']._serialized_start = 892
    _globals['_ACLPERMISSIONTYPE']._serialized_end = 1038