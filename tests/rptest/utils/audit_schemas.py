# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import jsonschema
import jsonschema.exceptions

PRODUCT_SCHEMA = {
    'type': 'object',
    'properties': {
        'name': {
            'type': 'string'
        },
        'vendor_name': {
            'type': 'string'
        },
        'version': {
            'type': 'string'
        }
    },
    'required': ['name', 'vendor_name', 'version'],
    'additionalProperties': False
}

USER_SCHEMA = {
    'type': 'object',
    'properties': {
        'credential_uid': {
            'type': 'string'
        },
        'domain': {
            'type': 'string'
        },
        'name': {
            'type': 'string'
        },
        'type_id': {
            'type': 'number',
            'enum': [0, 1, 2, 3, 99]
        }
    },
    'required': ['name', 'type_id'],
    'additionalProperties': False
}

OCSF_BASE_SCHEMA = {
    'type':
    'object',
    'properties': {
        'category_uid': {
            'type': 'number',
            'enum': [1, 2, 3, 4, 5, 6]
        },
        'class_uid': {
            'type':
            'number',
            'enum': [
                1001, 1002, 1003, 1004, 1005, 1006, 1007, 2001, 3001, 3002,
                3003, 3004, 3005, 3006, 4001, 4002, 4003, 4004, 4005, 4006,
                4007, 4008, 4009, 4010, 4011, 4012, 5001, 5002, 6001, 6002,
                6003, 6004
            ]
        },
        'count': {
            'type': 'number'
        },
        'end_time': {
            'type': 'number'
        },
        'metadata': {
            'type': 'object',
            'properties': {
                'version': {
                    'type': 'string'
                },
                'product': PRODUCT_SCHEMA
            },
            'required': ['product', 'version'],
            'additionalProperties': False
        },
        'severity_id': {
            'type': 'number',
            'enum': [0, 1, 2, 3, 4, 5, 6, 99]
        },
        'start_time': {
            'type': 'number'
        },
        'time': {
            'type': 'number'
        },
        'type_uid': {
            'type': 'number'
        }
    },
    'required': [
        'category_uid', 'class_uid', 'metadata', 'severity_id', 'time',
        'type_uid'
    ],
    'additionalProperties':
    False
}

ENDPOINT_SCHEMA = {
    'type': 'object',
    'properties': {
        'intermediate_ips': {
            'type': 'array'
        },
        'ip': {
            'type': 'string'
        },
        'name': {
            'type': 'string'
        },
        'port': {
            'type': 'number'
        },
        'svc_name': {
            'type': 'string'
        },
        'uid': {
            'type': 'string'
        }
    },
    'required': ['ip', 'port'],
    'additionalProperties': False
}

AUTH_METADATA_SCHEMA = {
    'type': 'object',
    'properties': {
        'acl_authorization': {
            'type': 'object',
            'properties': {
                'host': {
                    'type': 'string'
                },
                'op': {
                    'type': 'string'
                },
                'permission_type': {
                    'type': 'string'
                },
                'principal': {
                    'type': 'string'
                }
            },
            'required': ['host', 'op', 'permission_type', 'principal'],
            'additionalProperties': False
        },
        'resource': {
            'type': 'object',
            'properties': {
                'name': {
                    'type': 'string'
                },
                'pattern': {
                    'type': 'string'
                },
                'type': {
                    'type': 'string'
                }
            },
            'required': ['name', 'pattern', 'type'],
            'additionalProperties': False
        }
    },
    'required': ['acl_authorization', 'resource'],
    'additionalProperties': False
}

API_ACTIVITY_SCHEMA = {
    'type':
    'object',
    'properties': {
        'activity_id': {
            'type': 'number',
            'enum': [0, 1, 2, 3, 4, 99]
        },
        'actor': {
            'type': 'object',
            'properties': {
                'authorizations': {
                    'type': 'array',
                    'items': {
                        'type': 'object',
                        'properties': {
                            'decision': {
                                'type': 'string'
                            },
                            'policy': {
                                'type': 'object',
                                'properties': {
                                    'desc': {
                                        'type': 'string'
                                    },
                                    'name': {
                                        'type': 'string'
                                    }
                                },
                                'required': ['desc', 'name'],
                                'additionalProperties': False
                            },
                        },
                        'required': ['decision'],
                        'additionalProperties': False
                    }
                },
                'user': USER_SCHEMA
            },
            'required': ['authorizations', 'user'],
            'additionalProperties': False
        },
        'api': {
            'type': 'object',
            'properties': {
                'operation': {
                    'type': 'string'
                },
                'service': {
                    'type': 'object',
                    'properties': {
                        'name': {
                            'type': 'string'
                        }
                    },
                    'required': ['name']
                }
            },
            'required': ['operation', 'service'],
            'additionalProperties': False
        },
        'dst_endpoint': ENDPOINT_SCHEMA,
        'http_request': {
            'type':
            'object',
            'properties': {
                'http_headers': {
                    'type': 'array',
                    'items': {
                        'type': 'object',
                        'properties': {
                            'name': {
                                'type': 'string'
                            },
                            'value': {
                                'type': 'string'
                            }
                        },
                        'required': ['name', 'value'],
                        'additionalProperties': False
                    }
                },
                'http_method': {
                    'type': 'string'
                },
                'url': {
                    'type':
                    'object',
                    'properties': {
                        'hostname': {
                            'type': 'string'
                        },
                        'path': {
                            'type': 'string'
                        },
                        'port': {
                            'type': 'number'
                        },
                        'scheme': {
                            'type': 'string'
                        },
                        'url_string': {
                            'type': 'string'
                        }
                    },
                    'required':
                    ['hostname', 'path', 'port', 'scheme', 'url_string'],
                    'additionalProperties':
                    False
                },
                'user_agent': {
                    'type': 'string'
                },
                'version': {
                    'type': 'string'
                }
            },
            'required':
            ['http_headers', 'http_method', 'url', 'user_agent', 'version'],
            'additionalProperties':
            False
        },
        'resources': {
            'type': 'array',
            'items': {
                'type': 'object',
                'properties': {
                    'name': {
                        'type': 'string'
                    },
                    'type': {
                        'type': 'string'
                    }
                },
                'required': ['name', 'type'],
            },
            'additionalProperties': False
        },
        'src_endpoint': ENDPOINT_SCHEMA,
        'status_id': {
            'type': 'number',
            'enum': [0, 1, 2, 99]
        },
        'unmapped': {
            'type': 'object',
            'properties': {
                'authorization_metadata': AUTH_METADATA_SCHEMA
            },
            'required': [],
            'additionalProperties': False
        }
    },
    'required': [
        'activity_id', 'actor', 'api', 'dst_endpoint', 'src_endpoint',
        'status_id', 'unmapped'
    ],
    'additionalProperties':
    False
}

APPLICATION_ACTIVITY_SCHEMA = {
    'type': 'object',
    'properties': {
        'activity_id': {
            'type': 'number',
            'enum': [0, 1, 2, 3, 4, 99]
        },
        'app': PRODUCT_SCHEMA
    },
    'required': ['activity_id', 'app'],
    'additionalProperties': False
}

AUTHENTICATION_SCHEMA = {
    'type':
    'object',
    'properties': {
        'activity_id': {
            'type': 'number',
            'enum': [0, 1, 2, 3, 4, 99]
        },
        'auth_protocol': {
            'type': 'string'
        },
        'auth_protocol_id': {
            'type': 'number',
            'enum': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 99]
        },
        'dst_endpoint': ENDPOINT_SCHEMA,
        'is_cleartext': {
            'type': 'boolean'
        },
        'is_mfa': {
            'type': 'boolean'
        },
        'service': {
            'type': 'object',
            'properties': {
                'name': {
                    'type': 'string'
                }
            },
            'required': ['name']
        },
        'src_endpoint': ENDPOINT_SCHEMA,
        'status_id': {
            'type': 'number',
            'enum': [0, 1, 2, 99]
        },
        'status_detail': {
            'type': 'string'
        },
        'user': USER_SCHEMA
    },
    'required': [
        'activity_id', 'auth_protocol_id', 'dst_endpoint', 'is_cleartext',
        'is_mfa', 'service', 'src_endpoint', 'status_id'
    ],
    'additionalProperties':
    False
}


# A proper schema is an application based schema + the fields that exist
# in the OCSF base schema.
def _create_schema(schema):
    return {
        'type': 'object',
        'properties': schema['properties'] | OCSF_BASE_SCHEMA['properties'],
        'required': schema['required'] + OCSF_BASE_SCHEMA['required'],
        'additionalProperties': False
    }


# All schemas outside of this list are nested within the supported structures
# and are not expected to be observed as-is in the audit log
VALID_AUDIT_SCHEMAS = [
    _create_schema(schema) for schema in
    [API_ACTIVITY_SCHEMA, APPLICATION_ACTIVITY_SCHEMA, AUTHENTICATION_SCHEMA]
]


class AuditSchemeValidationException(Exception):
    def __init__(self, excs):
        failures = [str(exc) for exc in excs]
        self.message = '\n'.join(failures)


def validate_audit_contents(message):
    """
    This method verifies the validity of the contents of a message that
    has already passed the schema check.
    """
    if 'count' not in message:
        return 'start_time' not in message and 'end_time' not in message

    if message['count'] <= 1:
        return False
    if 'start_time' not in message:
        return False
    if 'end_time' not in message:
        return False
    return True


def validate_audit_schema(message):
    """
    Verify that a given message adheres to one of the supported audit log
    record formats and if one of those checks pass, that the fields within
    the records are themselves valid.
    """
    excs = []
    for schema in VALID_AUDIT_SCHEMAS:
        try:
            jsonschema.validate(instance=message, schema=schema)
            if not validate_audit_contents(message):
                excs = [
                    RuntimeError(
                        f'Message {message} failed to pass content validation, check "count", "start_time" and "end_time keys/values"'
                    )
                ]
                break
            return
        except Exception as e:
            excs.append(e)
    assert len(excs) > 0
    raise AuditSchemeValidationException(excs)
