"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(_runtime_version.Domain.PUBLIC, 5, 29, 0, '', 'proto/redpanda/core/admin/v2/shadow_link.proto')
_sym_db = _symbol_database.Default()
from ......proto.redpanda.core.pbgen import options_pb2 as proto_dot_redpanda_dot_core_dot_pbgen_dot_options__pb2
from ......proto.redpanda.core.pbgen import rpc_pb2 as proto_dot_redpanda_dot_core_dot_pbgen_dot_rpc__pb2
from ......proto.redpanda.core.common.v1 import acl_pb2 as proto_dot_redpanda_dot_core_dot_common_dot_v1_dot_acl__pb2
from ......proto.redpanda.core.common.v1 import tls_pb2 as proto_dot_redpanda_dot_core_dot_common_dot_v1_dot_tls__pb2
from ......google.api import field_behavior_pb2 as google_dot_api_dot_field__behavior__pb2
from ......google.api import field_info_pb2 as google_dot_api_dot_field__info__pb2
from ......google.api import resource_pb2 as google_dot_api_dot_resource__pb2
from google.protobuf import duration_pb2 as google_dot_protobuf_dot_duration__pb2
from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2
from google.protobuf import field_mask_pb2 as google_dot_protobuf_dot_field__mask__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n.proto/redpanda/core/admin/v2/shadow_link.proto\x12\x16redpanda.core.admin.v2\x1a\'proto/redpanda/core/pbgen/options.proto\x1a#proto/redpanda/core/pbgen/rpc.proto\x1a\'proto/redpanda/core/common/v1/acl.proto\x1a\'proto/redpanda/core/common/v1/tls.proto\x1a\x1fgoogle/api/field_behavior.proto\x1a\x1bgoogle/api/field_info.proto\x1a\x19google/api/resource.proto\x1a\x1egoogle/protobuf/duration.proto\x1a\x1fgoogle/protobuf/timestamp.proto\x1a google/protobuf/field_mask.proto"\xc2\x01\n\nShadowLink\x12\x11\n\x04name\x18\x01 \x01(\tB\x03\xe0A\x02\x12\x18\n\x03uid\x18\x02 \x01(\tB\x0b\xe0A\x03\xe2\x8c\xcf\xd7\x08\x02\x08\x01\x12H\n\x0econfigurations\x18\x03 \x01(\x0b20.redpanda.core.admin.v2.ShadowLinkConfigurations\x12=\n\x06status\x18\x04 \x01(\x0b2(.redpanda.core.admin.v2.ShadowLinkStatusB\x03\xe0A\x03"\xc0\x01\n\x0bShadowTopic\x12\x11\n\x04name\x18\x01 \x01(\tB\x03\xe0A\x03\x12\x1d\n\x08topic_id\x18\x02 \x01(\tB\x0b\xe0A\x03\xe2\x8c\xcf\xd7\x08\x02\x08\x01\x12\x19\n\x11source_topic_name\x18\x03 \x01(\t\x12$\n\x0fsource_topic_id\x18\x04 \x01(\tB\x0b\xe0A\x03\xe2\x8c\xcf\xd7\x08\x02\x08\x01\x12>\n\x06status\x18\x05 \x01(\x0b2).redpanda.core.admin.v2.ShadowTopicStatusB\x03\xe0A\x03"R\n\x17CreateShadowLinkRequest\x127\n\x0bshadow_link\x18\x01 \x01(\x0b2".redpanda.core.admin.v2.ShadowLink"S\n\x18CreateShadowLinkResponse\x127\n\x0bshadow_link\x18\x01 \x01(\x0b2".redpanda.core.admin.v2.ShadowLink"p\n\x17DeleteShadowLinkRequest\x12F\n\x04name\x18\x01 \x01(\tB8\xe0A\x02\xfaA2\n0redpanda.core.admin.ShadowLinkService/ShadowLink\x12\r\n\x05force\x18\x02 \x01(\x08"\x1a\n\x18DeleteShadowLinkResponse"^\n\x14GetShadowLinkRequest\x12F\n\x04name\x18\x01 \x01(\tB8\xe0A\x02\xfaA2\n0redpanda.core.admin.ShadowLinkService/ShadowLink"P\n\x15GetShadowLinkResponse\x127\n\x0bshadow_link\x18\x01 \x01(\x0b2".redpanda.core.admin.v2.ShadowLink"\x18\n\x16ListShadowLinksRequest"S\n\x17ListShadowLinksResponse\x128\n\x0cshadow_links\x18\x01 \x03(\x0b2".redpanda.core.admin.v2.ShadowLink"\x83\x01\n\x17UpdateShadowLinkRequest\x127\n\x0bshadow_link\x18\x01 \x01(\x0b2".redpanda.core.admin.v2.ShadowLink\x12/\n\x0bupdate_mask\x18\x02 \x01(\x0b2\x1a.google.protobuf.FieldMask"S\n\x18UpdateShadowLinkResponse\x127\n\x0bshadow_link\x18\x01 \x01(\x0b2".redpanda.core.admin.v2.ShadowLink"y\n\x0fFailOverRequest\x12F\n\x04name\x18\x01 \x01(\tB8\xe0A\x02\xfaA2\n0redpanda.core.admin.ShadowLinkService/ShadowLink\x12\x1e\n\x11shadow_topic_name\x18\x02 \x01(\tB\x03\xe0A\x01"K\n\x10FailOverResponse\x127\n\x0bshadow_link\x18\x01 \x01(\x0b2".redpanda.core.admin.v2.ShadowLink"I\n\x15GetShadowTopicRequest\x12\x1d\n\x10shadow_link_name\x18\x01 \x01(\tB\x03\xe0A\x02\x12\x11\n\x04name\x18\x02 \x01(\tB\x03\xe0A\x02"S\n\x16GetShadowTopicResponse\x129\n\x0cshadow_topic\x18\x01 \x01(\x0b2#.redpanda.core.admin.v2.ShadowTopic"8\n\x17ListShadowTopicsRequest\x12\x1d\n\x10shadow_link_name\x18\x01 \x01(\tB\x03\xe0A\x02"V\n\x18ListShadowTopicsResponse\x12:\n\rshadow_topics\x18\x01 \x03(\x0b2#.redpanda.core.admin.v2.ShadowTopic"\xc0\x03\n\x18ShadowLinkConfigurations\x12G\n\x0eclient_options\x18\x01 \x01(\x0b2/.redpanda.core.admin.v2.ShadowLinkClientOptions\x12U\n\x1btopic_metadata_sync_options\x18\x02 \x01(\x0b20.redpanda.core.admin.v2.TopicMetadataSyncOptions\x12W\n\x1cconsumer_offset_sync_options\x18\x03 \x01(\x0b21.redpanda.core.admin.v2.ConsumerOffsetSyncOptions\x12R\n\x15security_sync_options\x18\x04 \x01(\x0b23.redpanda.core.admin.v2.SecuritySettingsSyncOptions\x12W\n\x1cschema_registry_sync_options\x18\x05 \x01(\x0b21.redpanda.core.admin.v2.SchemaRegistrySyncOptions"\xb4\x06\n\x17ShadowLinkClientOptions\x12\x1e\n\x11bootstrap_servers\x18\x01 \x03(\tB\x03\xe0A\x02\x12\x16\n\tclient_id\x18\x02 \x01(\tB\x03\xe0A\x03\x12\x19\n\x11source_cluster_id\x18\x03 \x01(\t\x12?\n\x0ctls_settings\x18\x04 \x01(\x0b2$.redpanda.core.common.v1.TLSSettingsH\x00\x88\x01\x01\x12^\n\x1cauthentication_configuration\x18\x05 \x01(\x0b23.redpanda.core.admin.v2.AuthenticationConfigurationH\x01\x88\x01\x01\x12\x1b\n\x13metadata_max_age_ms\x18\x06 \x01(\x05\x12*\n\x1deffective_metadata_max_age_ms\x18\x07 \x01(\x05B\x03\xe0A\x03\x12\x1d\n\x15connection_timeout_ms\x18\x08 \x01(\x05\x12,\n\x1feffective_connection_timeout_ms\x18\t \x01(\x05B\x03\xe0A\x03\x12\x18\n\x10retry_backoff_ms\x18\n \x01(\x05\x12\'\n\x1aeffective_retry_backoff_ms\x18\x0b \x01(\x05B\x03\xe0A\x03\x12\x19\n\x11fetch_wait_max_ms\x18\x0c \x01(\x05\x12(\n\x1beffective_fetch_wait_max_ms\x18\r \x01(\x05B\x03\xe0A\x03\x12\x17\n\x0ffetch_min_bytes\x18\x0e \x01(\x05\x12&\n\x19effective_fetch_min_bytes\x18\x0f \x01(\x05B\x03\xe0A\x03\x12\x17\n\x0ffetch_max_bytes\x18\x10 \x01(\x05\x12&\n\x19effective_fetch_max_bytes\x18\x11 \x01(\x05B\x03\xe0A\x03\x12!\n\x19fetch_partition_max_bytes\x18\x12 \x01(\x05\x120\n#effective_fetch_partition_max_bytes\x18\x13 \x01(\x05B\x03\xe0A\x03B\x0f\n\r_tls_settingsB\x1f\n\x1d_authentication_configuration"\xc6\x04\n\x18TopicMetadataSyncOptions\x12+\n\x08interval\x18\x01 \x01(\x0b2\x19.google.protobuf.Duration\x12:\n\x12effective_interval\x18\x02 \x01(\x0b2\x19.google.protobuf.DurationB\x03\xe0A\x03\x12L\n auto_create_shadow_topic_filters\x18\x03 \x03(\x0b2".redpanda.core.admin.v2.NameFilter\x12&\n\x1esynced_shadow_topic_properties\x18\x04 \x03(\t\x12\x17\n\x0fexclude_default\x18\x05 \x01(\x08\x12\\\n\x11start_at_earliest\x18\x06 \x01(\x0b2?.redpanda.core.admin.v2.TopicMetadataSyncOptions.EarliestOffsetH\x00\x12X\n\x0fstart_at_latest\x18\x07 \x01(\x0b2=.redpanda.core.admin.v2.TopicMetadataSyncOptions.LatestOffsetH\x00\x128\n\x12start_at_timestamp\x18\x08 \x01(\x0b2\x1a.google.protobuf.TimestampH\x00\x12\x0e\n\x06paused\x18\t \x01(\x08\x1a\x10\n\x0eEarliestOffset\x1a\x0e\n\x0cLatestOffsetB\x0e\n\x0cstart_offset"\xcf\x01\n\x19SchemaRegistrySyncOptions\x12s\n\x1cshadow_schema_registry_topic\x18\x01 \x01(\x0b2K.redpanda.core.admin.v2.SchemaRegistrySyncOptions.ShadowSchemaRegistryTopicH\x00\x1a\x1b\n\x19ShadowSchemaRegistryTopicB \n\x1eschema_registry_shadowing_mode"\xcf\x01\n\x19ConsumerOffsetSyncOptions\x12+\n\x08interval\x18\x01 \x01(\x0b2\x19.google.protobuf.Duration\x12:\n\x12effective_interval\x18\x02 \x01(\x0b2\x19.google.protobuf.DurationB\x03\xe0A\x03\x12\x0e\n\x06paused\x18\x03 \x01(\x08\x129\n\rgroup_filters\x18\x04 \x03(\x0b2".redpanda.core.admin.v2.NameFilter"\xce\x01\n\x1bSecuritySettingsSyncOptions\x12+\n\x08interval\x18\x01 \x01(\x0b2\x19.google.protobuf.Duration\x12:\n\x12effective_interval\x18\x02 \x01(\x0b2\x19.google.protobuf.DurationB\x03\xe0A\x03\x12\x0e\n\x06paused\x18\x03 \x01(\x08\x126\n\x0bacl_filters\x18\x04 \x03(\x0b2!.redpanda.core.admin.v2.ACLFilter"\xb7\x01\n\x1bAuthenticationConfiguration\x12B\n\x13scram_configuration\x18\x01 \x01(\x0b2#.redpanda.core.admin.v2.ScramConfigH\x00\x12B\n\x13plain_configuration\x18\x02 \x01(\x0b2#.redpanda.core.admin.v2.PlainConfigH\x00B\x10\n\x0eauthentication"\x8e\x01\n\x0bPlainConfig\x12\x10\n\x08username\x18\x01 \x01(\t\x12\x18\n\x08password\x18\x02 \x01(\tB\x06\x80\x01\x01\xe0A\x04\x12\x19\n\x0cpassword_set\x18\x03 \x01(\x08B\x03\xe0A\x03\x128\n\x0fpassword_set_at\x18\x04 \x01(\x0b2\x1a.google.protobuf.TimestampB\x03\xe0A\x03"\xcf\x01\n\x0bScramConfig\x12\x10\n\x08username\x18\x01 \x01(\t\x12\x18\n\x08password\x18\x02 \x01(\tB\x06\x80\x01\x01\xe0A\x04\x12\x19\n\x0cpassword_set\x18\x03 \x01(\x08B\x03\xe0A\x03\x128\n\x0fpassword_set_at\x18\x04 \x01(\x0b2\x1a.google.protobuf.TimestampB\x03\xe0A\x03\x12?\n\x0fscram_mechanism\x18\x05 \x01(\x0e2&.redpanda.core.admin.v2.ScramMechanism"\x8e\x01\n\nNameFilter\x129\n\x0cpattern_type\x18\x01 \x01(\x0e2#.redpanda.core.admin.v2.PatternType\x127\n\x0bfilter_type\x18\x02 \x01(\x0e2".redpanda.core.admin.v2.FilterType\x12\x0c\n\x04name\x18\x03 \x01(\t"\x8f\x01\n\tACLFilter\x12B\n\x0fresource_filter\x18\x01 \x01(\x0b2).redpanda.core.admin.v2.ACLResourceFilter\x12>\n\raccess_filter\x18\x02 \x01(\x0b2\'.redpanda.core.admin.v2.ACLAccessFilter"\x99\x01\n\x11ACLResourceFilter\x12;\n\rresource_type\x18\x01 \x01(\x0e2$.redpanda.core.common.v1.ACLResource\x129\n\x0cpattern_type\x18\x02 \x01(\x0e2#.redpanda.core.common.v1.ACLPattern\x12\x0c\n\x04name\x18\x03 \x01(\t"\xb1\x01\n\x0fACLAccessFilter\x12\x11\n\tprincipal\x18\x01 \x01(\t\x128\n\toperation\x18\x02 \x01(\x0e2%.redpanda.core.common.v1.ACLOperation\x12C\n\x0fpermission_type\x18\x03 \x01(\x0e2*.redpanda.core.common.v1.ACLPermissionType\x12\x0c\n\x04host\x18\x04 \x01(\t"\xf3\x01\n\x10ShadowLinkStatus\x126\n\x05state\x18\x01 \x01(\x0e2\'.redpanda.core.admin.v2.ShadowLinkState\x12C\n\rtask_statuses\x18\x02 \x03(\x0b2,.redpanda.core.admin.v2.ShadowLinkTaskStatus\x12:\n\rshadow_topics\x18\x03 \x03(\x0b2#.redpanda.core.admin.v2.ShadowTopic\x12&\n\x1esynced_shadow_topic_properties\x18\x04 \x03(\t"\x88\x01\n\x14ShadowLinkTaskStatus\x12\x0c\n\x04name\x18\x01 \x01(\t\x120\n\x05state\x18\x02 \x01(\x0e2!.redpanda.core.admin.v2.TaskState\x12\x0e\n\x06reason\x18\x03 \x01(\t\x12\x11\n\tbroker_id\x18\x04 \x01(\x05\x12\r\n\x05shard\x18\x05 \x01(\x05"\x9e\x01\n\x11ShadowTopicStatus\x127\n\x05state\x18\x01 \x01(\x0e2(.redpanda.core.admin.v2.ShadowTopicState\x12P\n\x15partition_information\x18\x02 \x03(\x0b21.redpanda.core.admin.v2.TopicPartitionInformation"\xce\x01\n\x19TopicPartitionInformation\x12\x14\n\x0cpartition_id\x18\x01 \x01(\x03\x12!\n\x19source_last_stable_offset\x18\x02 \x01(\x03\x12\x1d\n\x15source_high_watermark\x18\x03 \x01(\x03\x12\x16\n\x0ehigh_watermark\x18\x04 \x01(\x03\x12A\n\x1dsource_last_updated_timestamp\x18\x05 \x01(\x0b2\x1a.google.protobuf.Timestamp*p\n\x0fShadowLinkState\x12!\n\x1dSHADOW_LINK_STATE_UNSPECIFIED\x10\x00\x12\x1c\n\x18SHADOW_LINK_STATE_ACTIVE\x10\x01\x12\x1c\n\x18SHADOW_LINK_STATE_PAUSED\x10\x02*w\n\x0eScramMechanism\x12\x1f\n\x1bSCRAM_MECHANISM_UNSPECIFIED\x10\x00\x12!\n\x1dSCRAM_MECHANISM_SCRAM_SHA_256\x10\x01\x12!\n\x1dSCRAM_MECHANISM_SCRAM_SHA_512\x10\x02*}\n\x0bPatternType\x12\x1c\n\x18PATTERN_TYPE_UNSPECIFIED\x10\x00\x12\x18\n\x14PATTERN_TYPE_LITERAL\x10\x01\x12\x17\n\x13PATTERN_TYPE_PREFIX\x10\x02\x12\x19\n\x15PATTERN_TYPE_PREFIXED\x10\x02\x1a\x02\x10\x01*[\n\nFilterType\x12\x1b\n\x17FILTER_TYPE_UNSPECIFIED\x10\x00\x12\x17\n\x13FILTER_TYPE_INCLUDE\x10\x01\x12\x17\n\x13FILTER_TYPE_EXCLUDE\x10\x02*\xaa\x01\n\tTaskState\x12\x1a\n\x16TASK_STATE_UNSPECIFIED\x10\x00\x12\x15\n\x11TASK_STATE_ACTIVE\x10\x01\x12\x15\n\x11TASK_STATE_PAUSED\x10\x02\x12\x1f\n\x1bTASK_STATE_LINK_UNAVAILABLE\x10\x03\x12\x1a\n\x16TASK_STATE_NOT_RUNNING\x10\x04\x12\x16\n\x12TASK_STATE_FAULTED\x10\x05*\xa0\x02\n\x10ShadowTopicState\x12"\n\x1eSHADOW_TOPIC_STATE_UNSPECIFIED\x10\x00\x12\x1d\n\x19SHADOW_TOPIC_STATE_ACTIVE\x10\x01\x12\x1e\n\x1aSHADOW_TOPIC_STATE_FAULTED\x10\x02\x12\x1d\n\x19SHADOW_TOPIC_STATE_PAUSED\x10\x03\x12#\n\x1fSHADOW_TOPIC_STATE_FAILING_OVER\x10\x04\x12"\n\x1eSHADOW_TOPIC_STATE_FAILED_OVER\x10\x05\x12 \n\x1cSHADOW_TOPIC_STATE_PROMOTING\x10\x06\x12\x1f\n\x1bSHADOW_TOPIC_STATE_PROMOTED\x10\x072\xe1\x07\n\x11ShadowLinkService\x12}\n\x10CreateShadowLink\x12/.redpanda.core.admin.v2.CreateShadowLinkRequest\x1a0.redpanda.core.admin.v2.CreateShadowLinkResponse"\x06\xea\x92\x19\x02\x10\x03\x12}\n\x10DeleteShadowLink\x12/.redpanda.core.admin.v2.DeleteShadowLinkRequest\x1a0.redpanda.core.admin.v2.DeleteShadowLinkResponse"\x06\xea\x92\x19\x02\x10\x03\x12t\n\rGetShadowLink\x12,.redpanda.core.admin.v2.GetShadowLinkRequest\x1a-.redpanda.core.admin.v2.GetShadowLinkResponse"\x06\xea\x92\x19\x02\x10\x03\x12z\n\x0fListShadowLinks\x12..redpanda.core.admin.v2.ListShadowLinksRequest\x1a/.redpanda.core.admin.v2.ListShadowLinksResponse"\x06\xea\x92\x19\x02\x10\x03\x12}\n\x10UpdateShadowLink\x12/.redpanda.core.admin.v2.UpdateShadowLinkRequest\x1a0.redpanda.core.admin.v2.UpdateShadowLinkResponse"\x06\xea\x92\x19\x02\x10\x03\x12e\n\x08FailOver\x12\'.redpanda.core.admin.v2.FailOverRequest\x1a(.redpanda.core.admin.v2.FailOverResponse"\x06\xea\x92\x19\x02\x10\x03\x12w\n\x0eGetShadowTopic\x12-.redpanda.core.admin.v2.GetShadowTopicRequest\x1a..redpanda.core.admin.v2.GetShadowTopicResponse"\x06\xea\x92\x19\x02\x10\x03\x12}\n\x10ListShadowTopics\x12/.redpanda.core.admin.v2.ListShadowTopicsRequest\x1a0.redpanda.core.admin.v2.ListShadowTopicsResponse"\x06\xea\x92\x19\x02\x10\x03B\x10\xea\x92\x19\x0cproto::adminb\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'proto.redpanda.core.admin.v2.shadow_link_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    _globals['DESCRIPTOR']._loaded_options = None
    _globals['DESCRIPTOR']._serialized_options = b'\xea\x92\x19\x0cproto::admin'
    _globals['_PATTERNTYPE']._loaded_options = None
    _globals['_PATTERNTYPE']._serialized_options = b'\x10\x01'
    _globals['_SHADOWLINK'].fields_by_name['name']._loaded_options = None
    _globals['_SHADOWLINK'].fields_by_name['name']._serialized_options = b'\xe0A\x02'
    _globals['_SHADOWLINK'].fields_by_name['uid']._loaded_options = None
    _globals['_SHADOWLINK'].fields_by_name['uid']._serialized_options = b'\xe0A\x03\xe2\x8c\xcf\xd7\x08\x02\x08\x01'
    _globals['_SHADOWLINK'].fields_by_name['status']._loaded_options = None
    _globals['_SHADOWLINK'].fields_by_name['status']._serialized_options = b'\xe0A\x03'
    _globals['_SHADOWTOPIC'].fields_by_name['name']._loaded_options = None
    _globals['_SHADOWTOPIC'].fields_by_name['name']._serialized_options = b'\xe0A\x03'
    _globals['_SHADOWTOPIC'].fields_by_name['topic_id']._loaded_options = None
    _globals['_SHADOWTOPIC'].fields_by_name['topic_id']._serialized_options = b'\xe0A\x03\xe2\x8c\xcf\xd7\x08\x02\x08\x01'
    _globals['_SHADOWTOPIC'].fields_by_name['source_topic_id']._loaded_options = None
    _globals['_SHADOWTOPIC'].fields_by_name['source_topic_id']._serialized_options = b'\xe0A\x03\xe2\x8c\xcf\xd7\x08\x02\x08\x01'
    _globals['_SHADOWTOPIC'].fields_by_name['status']._loaded_options = None
    _globals['_SHADOWTOPIC'].fields_by_name['status']._serialized_options = b'\xe0A\x03'
    _globals['_DELETESHADOWLINKREQUEST'].fields_by_name['name']._loaded_options = None
    _globals['_DELETESHADOWLINKREQUEST'].fields_by_name['name']._serialized_options = b'\xe0A\x02\xfaA2\n0redpanda.core.admin.ShadowLinkService/ShadowLink'
    _globals['_GETSHADOWLINKREQUEST'].fields_by_name['name']._loaded_options = None
    _globals['_GETSHADOWLINKREQUEST'].fields_by_name['name']._serialized_options = b'\xe0A\x02\xfaA2\n0redpanda.core.admin.ShadowLinkService/ShadowLink'
    _globals['_FAILOVERREQUEST'].fields_by_name['name']._loaded_options = None
    _globals['_FAILOVERREQUEST'].fields_by_name['name']._serialized_options = b'\xe0A\x02\xfaA2\n0redpanda.core.admin.ShadowLinkService/ShadowLink'
    _globals['_FAILOVERREQUEST'].fields_by_name['shadow_topic_name']._loaded_options = None
    _globals['_FAILOVERREQUEST'].fields_by_name['shadow_topic_name']._serialized_options = b'\xe0A\x01'
    _globals['_GETSHADOWTOPICREQUEST'].fields_by_name['shadow_link_name']._loaded_options = None
    _globals['_GETSHADOWTOPICREQUEST'].fields_by_name['shadow_link_name']._serialized_options = b'\xe0A\x02'
    _globals['_GETSHADOWTOPICREQUEST'].fields_by_name['name']._loaded_options = None
    _globals['_GETSHADOWTOPICREQUEST'].fields_by_name['name']._serialized_options = b'\xe0A\x02'
    _globals['_LISTSHADOWTOPICSREQUEST'].fields_by_name['shadow_link_name']._loaded_options = None
    _globals['_LISTSHADOWTOPICSREQUEST'].fields_by_name['shadow_link_name']._serialized_options = b'\xe0A\x02'
    _globals['_SHADOWLINKCLIENTOPTIONS'].fields_by_name['bootstrap_servers']._loaded_options = None
    _globals['_SHADOWLINKCLIENTOPTIONS'].fields_by_name['bootstrap_servers']._serialized_options = b'\xe0A\x02'
    _globals['_SHADOWLINKCLIENTOPTIONS'].fields_by_name['client_id']._loaded_options = None
    _globals['_SHADOWLINKCLIENTOPTIONS'].fields_by_name['client_id']._serialized_options = b'\xe0A\x03'
    _globals['_SHADOWLINKCLIENTOPTIONS'].fields_by_name['effective_metadata_max_age_ms']._loaded_options = None
    _globals['_SHADOWLINKCLIENTOPTIONS'].fields_by_name['effective_metadata_max_age_ms']._serialized_options = b'\xe0A\x03'
    _globals['_SHADOWLINKCLIENTOPTIONS'].fields_by_name['effective_connection_timeout_ms']._loaded_options = None
    _globals['_SHADOWLINKCLIENTOPTIONS'].fields_by_name['effective_connection_timeout_ms']._serialized_options = b'\xe0A\x03'
    _globals['_SHADOWLINKCLIENTOPTIONS'].fields_by_name['effective_retry_backoff_ms']._loaded_options = None
    _globals['_SHADOWLINKCLIENTOPTIONS'].fields_by_name['effective_retry_backoff_ms']._serialized_options = b'\xe0A\x03'
    _globals['_SHADOWLINKCLIENTOPTIONS'].fields_by_name['effective_fetch_wait_max_ms']._loaded_options = None
    _globals['_SHADOWLINKCLIENTOPTIONS'].fields_by_name['effective_fetch_wait_max_ms']._serialized_options = b'\xe0A\x03'
    _globals['_SHADOWLINKCLIENTOPTIONS'].fields_by_name['effective_fetch_min_bytes']._loaded_options = None
    _globals['_SHADOWLINKCLIENTOPTIONS'].fields_by_name['effective_fetch_min_bytes']._serialized_options = b'\xe0A\x03'
    _globals['_SHADOWLINKCLIENTOPTIONS'].fields_by_name['effective_fetch_max_bytes']._loaded_options = None
    _globals['_SHADOWLINKCLIENTOPTIONS'].fields_by_name['effective_fetch_max_bytes']._serialized_options = b'\xe0A\x03'
    _globals['_SHADOWLINKCLIENTOPTIONS'].fields_by_name['effective_fetch_partition_max_bytes']._loaded_options = None
    _globals['_SHADOWLINKCLIENTOPTIONS'].fields_by_name['effective_fetch_partition_max_bytes']._serialized_options = b'\xe0A\x03'
    _globals['_TOPICMETADATASYNCOPTIONS'].fields_by_name['effective_interval']._loaded_options = None
    _globals['_TOPICMETADATASYNCOPTIONS'].fields_by_name['effective_interval']._serialized_options = b'\xe0A\x03'
    _globals['_CONSUMEROFFSETSYNCOPTIONS'].fields_by_name['effective_interval']._loaded_options = None
    _globals['_CONSUMEROFFSETSYNCOPTIONS'].fields_by_name['effective_interval']._serialized_options = b'\xe0A\x03'
    _globals['_SECURITYSETTINGSSYNCOPTIONS'].fields_by_name['effective_interval']._loaded_options = None
    _globals['_SECURITYSETTINGSSYNCOPTIONS'].fields_by_name['effective_interval']._serialized_options = b'\xe0A\x03'
    _globals['_PLAINCONFIG'].fields_by_name['password']._loaded_options = None
    _globals['_PLAINCONFIG'].fields_by_name['password']._serialized_options = b'\x80\x01\x01\xe0A\x04'
    _globals['_PLAINCONFIG'].fields_by_name['password_set']._loaded_options = None
    _globals['_PLAINCONFIG'].fields_by_name['password_set']._serialized_options = b'\xe0A\x03'
    _globals['_PLAINCONFIG'].fields_by_name['password_set_at']._loaded_options = None
    _globals['_PLAINCONFIG'].fields_by_name['password_set_at']._serialized_options = b'\xe0A\x03'
    _globals['_SCRAMCONFIG'].fields_by_name['password']._loaded_options = None
    _globals['_SCRAMCONFIG'].fields_by_name['password']._serialized_options = b'\x80\x01\x01\xe0A\x04'
    _globals['_SCRAMCONFIG'].fields_by_name['password_set']._loaded_options = None
    _globals['_SCRAMCONFIG'].fields_by_name['password_set']._serialized_options = b'\xe0A\x03'
    _globals['_SCRAMCONFIG'].fields_by_name['password_set_at']._loaded_options = None
    _globals['_SCRAMCONFIG'].fields_by_name['password_set_at']._serialized_options = b'\xe0A\x03'
    _globals['_SHADOWLINKSERVICE'].methods_by_name['CreateShadowLink']._loaded_options = None
    _globals['_SHADOWLINKSERVICE'].methods_by_name['CreateShadowLink']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_SHADOWLINKSERVICE'].methods_by_name['DeleteShadowLink']._loaded_options = None
    _globals['_SHADOWLINKSERVICE'].methods_by_name['DeleteShadowLink']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_SHADOWLINKSERVICE'].methods_by_name['GetShadowLink']._loaded_options = None
    _globals['_SHADOWLINKSERVICE'].methods_by_name['GetShadowLink']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_SHADOWLINKSERVICE'].methods_by_name['ListShadowLinks']._loaded_options = None
    _globals['_SHADOWLINKSERVICE'].methods_by_name['ListShadowLinks']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_SHADOWLINKSERVICE'].methods_by_name['UpdateShadowLink']._loaded_options = None
    _globals['_SHADOWLINKSERVICE'].methods_by_name['UpdateShadowLink']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_SHADOWLINKSERVICE'].methods_by_name['FailOver']._loaded_options = None
    _globals['_SHADOWLINKSERVICE'].methods_by_name['FailOver']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_SHADOWLINKSERVICE'].methods_by_name['GetShadowTopic']._loaded_options = None
    _globals['_SHADOWLINKSERVICE'].methods_by_name['GetShadowTopic']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_SHADOWLINKSERVICE'].methods_by_name['ListShadowTopics']._loaded_options = None
    _globals['_SHADOWLINKSERVICE'].methods_by_name['ListShadowTopics']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_SHADOWLINKSTATE']._serialized_start = 6550
    _globals['_SHADOWLINKSTATE']._serialized_end = 6662
    _globals['_SCRAMMECHANISM']._serialized_start = 6664
    _globals['_SCRAMMECHANISM']._serialized_end = 6783
    _globals['_PATTERNTYPE']._serialized_start = 6785
    _globals['_PATTERNTYPE']._serialized_end = 6910
    _globals['_FILTERTYPE']._serialized_start = 6912
    _globals['_FILTERTYPE']._serialized_end = 7003
    _globals['_TASKSTATE']._serialized_start = 7006
    _globals['_TASKSTATE']._serialized_end = 7176
    _globals['_SHADOWTOPICSTATE']._serialized_start = 7179
    _globals['_SHADOWTOPICSTATE']._serialized_end = 7467
    _globals['_SHADOWLINK']._serialized_start = 423
    _globals['_SHADOWLINK']._serialized_end = 617
    _globals['_SHADOWTOPIC']._serialized_start = 620
    _globals['_SHADOWTOPIC']._serialized_end = 812
    _globals['_CREATESHADOWLINKREQUEST']._serialized_start = 814
    _globals['_CREATESHADOWLINKREQUEST']._serialized_end = 896
    _globals['_CREATESHADOWLINKRESPONSE']._serialized_start = 898
    _globals['_CREATESHADOWLINKRESPONSE']._serialized_end = 981
    _globals['_DELETESHADOWLINKREQUEST']._serialized_start = 983
    _globals['_DELETESHADOWLINKREQUEST']._serialized_end = 1095
    _globals['_DELETESHADOWLINKRESPONSE']._serialized_start = 1097
    _globals['_DELETESHADOWLINKRESPONSE']._serialized_end = 1123
    _globals['_GETSHADOWLINKREQUEST']._serialized_start = 1125
    _globals['_GETSHADOWLINKREQUEST']._serialized_end = 1219
    _globals['_GETSHADOWLINKRESPONSE']._serialized_start = 1221
    _globals['_GETSHADOWLINKRESPONSE']._serialized_end = 1301
    _globals['_LISTSHADOWLINKSREQUEST']._serialized_start = 1303
    _globals['_LISTSHADOWLINKSREQUEST']._serialized_end = 1327
    _globals['_LISTSHADOWLINKSRESPONSE']._serialized_start = 1329
    _globals['_LISTSHADOWLINKSRESPONSE']._serialized_end = 1412
    _globals['_UPDATESHADOWLINKREQUEST']._serialized_start = 1415
    _globals['_UPDATESHADOWLINKREQUEST']._serialized_end = 1546
    _globals['_UPDATESHADOWLINKRESPONSE']._serialized_start = 1548
    _globals['_UPDATESHADOWLINKRESPONSE']._serialized_end = 1631
    _globals['_FAILOVERREQUEST']._serialized_start = 1633
    _globals['_FAILOVERREQUEST']._serialized_end = 1754
    _globals['_FAILOVERRESPONSE']._serialized_start = 1756
    _globals['_FAILOVERRESPONSE']._serialized_end = 1831
    _globals['_GETSHADOWTOPICREQUEST']._serialized_start = 1833
    _globals['_GETSHADOWTOPICREQUEST']._serialized_end = 1906
    _globals['_GETSHADOWTOPICRESPONSE']._serialized_start = 1908
    _globals['_GETSHADOWTOPICRESPONSE']._serialized_end = 1991
    _globals['_LISTSHADOWTOPICSREQUEST']._serialized_start = 1993
    _globals['_LISTSHADOWTOPICSREQUEST']._serialized_end = 2049
    _globals['_LISTSHADOWTOPICSRESPONSE']._serialized_start = 2051
    _globals['_LISTSHADOWTOPICSRESPONSE']._serialized_end = 2137
    _globals['_SHADOWLINKCONFIGURATIONS']._serialized_start = 2140
    _globals['_SHADOWLINKCONFIGURATIONS']._serialized_end = 2588
    _globals['_SHADOWLINKCLIENTOPTIONS']._serialized_start = 2591
    _globals['_SHADOWLINKCLIENTOPTIONS']._serialized_end = 3411
    _globals['_TOPICMETADATASYNCOPTIONS']._serialized_start = 3414
    _globals['_TOPICMETADATASYNCOPTIONS']._serialized_end = 3996
    _globals['_TOPICMETADATASYNCOPTIONS_EARLIESTOFFSET']._serialized_start = 3948
    _globals['_TOPICMETADATASYNCOPTIONS_EARLIESTOFFSET']._serialized_end = 3964
    _globals['_TOPICMETADATASYNCOPTIONS_LATESTOFFSET']._serialized_start = 3966
    _globals['_TOPICMETADATASYNCOPTIONS_LATESTOFFSET']._serialized_end = 3980
    _globals['_SCHEMAREGISTRYSYNCOPTIONS']._serialized_start = 3999
    _globals['_SCHEMAREGISTRYSYNCOPTIONS']._serialized_end = 4206
    _globals['_SCHEMAREGISTRYSYNCOPTIONS_SHADOWSCHEMAREGISTRYTOPIC']._serialized_start = 4145
    _globals['_SCHEMAREGISTRYSYNCOPTIONS_SHADOWSCHEMAREGISTRYTOPIC']._serialized_end = 4172
    _globals['_CONSUMEROFFSETSYNCOPTIONS']._serialized_start = 4209
    _globals['_CONSUMEROFFSETSYNCOPTIONS']._serialized_end = 4416
    _globals['_SECURITYSETTINGSSYNCOPTIONS']._serialized_start = 4419
    _globals['_SECURITYSETTINGSSYNCOPTIONS']._serialized_end = 4625
    _globals['_AUTHENTICATIONCONFIGURATION']._serialized_start = 4628
    _globals['_AUTHENTICATIONCONFIGURATION']._serialized_end = 4811
    _globals['_PLAINCONFIG']._serialized_start = 4814
    _globals['_PLAINCONFIG']._serialized_end = 4956
    _globals['_SCRAMCONFIG']._serialized_start = 4959
    _globals['_SCRAMCONFIG']._serialized_end = 5166
    _globals['_NAMEFILTER']._serialized_start = 5169
    _globals['_NAMEFILTER']._serialized_end = 5311
    _globals['_ACLFILTER']._serialized_start = 5314
    _globals['_ACLFILTER']._serialized_end = 5457
    _globals['_ACLRESOURCEFILTER']._serialized_start = 5460
    _globals['_ACLRESOURCEFILTER']._serialized_end = 5613
    _globals['_ACLACCESSFILTER']._serialized_start = 5616
    _globals['_ACLACCESSFILTER']._serialized_end = 5793
    _globals['_SHADOWLINKSTATUS']._serialized_start = 5796
    _globals['_SHADOWLINKSTATUS']._serialized_end = 6039
    _globals['_SHADOWLINKTASKSTATUS']._serialized_start = 6042
    _globals['_SHADOWLINKTASKSTATUS']._serialized_end = 6178
    _globals['_SHADOWTOPICSTATUS']._serialized_start = 6181
    _globals['_SHADOWTOPICSTATUS']._serialized_end = 6339
    _globals['_TOPICPARTITIONINFORMATION']._serialized_start = 6342
    _globals['_TOPICPARTITIONINFORMATION']._serialized_end = 6548
    _globals['_SHADOWLINKSERVICE']._serialized_start = 7470
    _globals['_SHADOWLINKSERVICE']._serialized_end = 8463