from kafka.admin import (KafkaAdminClient, ConfigResource, ConfigResourceType,
                         NewTopic, ACLFilter, ACLOperation,
                         ResourcePatternFilter, ResourceType,
                         ACLResourcePatternType, ACLPermissionType)
import argparse


class Topic:
    def __init__(self, name, configs):
        configurations = {}
        for value in configs:
            configurations[value[0]] = value[1]
        self.name = name
        self.num_partitions = int(
            next((x for x in configs if x[0] == 'partition_count'), None)[1])
        self.replication_factor = int(
            next((x for x in configs if x[0] == 'replication_factor'),
                 None)[1])
        self.configs = configurations


def main():
    params = generate_options()
    options, program_options = params.parse_known_args()
    if options.copy_topic is None and options.copy_acl is None:
        print(f'you need to put --copy-acl or/and --copy-topic')
        return
    admin_origin = create_admin(options.host_origin, options.user_origin,
                                options.password_origin, options.ca_origin,
                                options.cert_origin, options.key_origin)

    admin_target = create_admin(options.host_target, options.user_target,
                                options.password_target, options.ca_target,
                                options.cert_target, options.key_target)
    if options.clean_target:
        clean_topic_acl_target(admin_target)
    if options.copy_acl and options.clean_target is None:
        copy_acl(admin_origin, admin_target)
    if options.copy_topic and options.clean_target is None:
        copy_topic(admin_origin, admin_target)
    print('success')


def clean_topic_acl_target(admin_target: KafkaAdminClient):
    origin = admin_target.list_topics()
    admin_target.delete_topics(origin)
    admin_target.delete_acls([
        ACLFilter(principal=None,
                  operation=ACLOperation.ANY,
                  resource_pattern=ResourcePatternFilter(
                      resource_type=ResourceType.ANY,
                      pattern_type=ACLResourcePatternType.ANY,
                      resource_name=None),
                  permission_type=ACLPermissionType.ANY,
                  host=None)
    ])


def copy_topic(admin_origin: KafkaAdminClient, admin_target: KafkaAdminClient):
    origin = admin_origin.list_topics()
    config = admin_origin.describe_configs(
        config_resources=map(topic_config, origin))
    topic_configs = config[0].resources
    new_topics = [create_topic(Topic(s[3], s[4])) for s in topic_configs]
    admin_target.create_topics(new_topics=new_topics)


def copy_acl(admin_origin: KafkaAdminClient, admin_target: KafkaAdminClient):
    origin = get_acls(admin_origin)[0]
    admin_target.create_acls(acls=origin)


def create_admin(host_origin, user_origin, password_origin, ca_origin,
                 cert_origin, key_origin):
    redpanda_security_protocol = 'SASL_SSL'
    redpanda_sasl_mechanism = 'SCRAM-SHA-256'

    return KafkaAdminClient(
        bootstrap_servers=host_origin,
        security_protocol=redpanda_security_protocol,
        sasl_mechanism=redpanda_sasl_mechanism,
        sasl_plain_username=user_origin,
        sasl_plain_password=password_origin,
        request_timeout_ms=3000000,
        api_version_auto_timeout_ms=3000,
        # Ensure you download the Kafka-API certs from the security tab
        ssl_cafile=ca_origin,
        ssl_certfile=cert_origin,
        ssl_keyfile=key_origin)


def topic_config(topic_name: str):
    return ConfigResource(ConfigResourceType.TOPIC, topic_name)


def create_topic(topic: Topic):
    return NewTopic(name=topic.name,
                    num_partitions=topic.num_partitions,
                    replication_factor=topic.replication_factor,
                    replica_assignments=None,
                    topic_configs=topic.configs)


def get_acls(admin: KafkaAdminClient):
    acl_filter = ACLFilter(principal=None,
                           operation=ACLOperation.ANY,
                           resource_pattern=ResourcePatternFilter(
                               resource_type=ResourceType.ANY,
                               pattern_type=ACLResourcePatternType.ANY,
                               resource_name=None),
                           permission_type=ACLPermissionType.ANY,
                           host=None)
    return admin.describe_acls(acl_filter)


def generate_options():
    parser = argparse.ArgumentParser(
        description=
        'move topic definition from one cluster to another one (no data)')

    parser.add_argument('--host-origin', type=str, required=True)

    parser.add_argument(
        '--user-origin',
        type=str,
        required=True,
    )

    parser.add_argument(
        '--password-origin',
        type=str,
        required=True,
    )

    parser.add_argument('--ca-origin', type=str, required=True)

    parser.add_argument('--key-origin', type=str)

    parser.add_argument('--cert-origin', type=str)

    parser.add_argument('--host-target', type=str, required=True)

    parser.add_argument(
        '--user-target',
        type=str,
        required=True,
    )

    parser.add_argument(
        '--password-target',
        type=str,
        required=True,
    )

    parser.add_argument('--ca-target', type=str, required=True)

    parser.add_argument('--key-target', type=str)

    parser.add_argument('--cert-target', type=str)

    parser.add_argument('--copy-topic', type=bool)

    parser.add_argument('--copy-acl', type=bool)

    parser.add_argument('--clean-target', type=bool)
    return parser


if __name__ == '__main__':
    main()
