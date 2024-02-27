import argparse
import json
import logging
import numpy
import random
import string
import sys
import time

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy

from kafka import KafkaAdminClient
from kafka.errors import NoBrokersAvailable
from kafka.admin import NewTopic


def setup_logger():
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    handler.setLevel(logging.DEBUG)
    logger = logging.getLogger("SerdeClient")
    logger.addHandler(handler)

    return logger


def write_json(ioclass, data):
    ioclass.write(json.dumps(data))
    ioclass.write('\n')
    ioclass.flush()


class TopicSwarm():
    def __init__(self, brokers, workers, issilent, logger):
        self.logger = logger
        self.brokers = brokers
        self.workers = workers
        self.issilent = issilent

    @staticmethod
    def generate_topic_name(prefix, length) -> str:
        gen_size = length - len(prefix)
        if gen_size < 2:
            raise RuntimeError(f"Topic name length is too low ({length}) "
                               "for generating it using prefix "
                               f"of '{prefix}'")

        suffix = ''.join(
            random.choices(string.ascii_letters + string.digits, k=gen_size))
        return f"{prefix}-{suffix}"

    def set_topic_config(self, name_length, skip_randomize):
        self.name_length = name_length
        self.skip_topic_names_randomization = skip_randomize

    def create_many_topics(self,
                           target_count,
                           topic_name_prefix="topic-swarm",
                           use_kafka_batch=True,
                           batch_size=256,
                           num_partitions=1,
                           num_replicas=3) -> tuple[list, dict]:
        """
            Creates topics using kafak admin lib in batches
            while tracking time spent to create

            target_count: Target number of topics to be created
            use_kafka_batch: Send whole batch in single kafka.client request
            batch_size: How many topics to be created in single batch
            num_partitions: How many partitions for each topic
            num_replicas: How many replicas for each topic

            return value: topic details list
        """
        def make_superuser_client():
            return KafkaAdminClient(
                bootstrap_servers=self.brokers,
                request_timeout_ms=30000,
                api_version_auto_timeout_ms=3000)

        # Kafka client
        self.logger.info("Creating kafka client")
        kclient = make_superuser_client()

        def _create_single_topic(topic_item):
            # Create topic with time tracking
            create_start_s = time.time()
            # Create kafka client Topic Spec
            newTopic = NewTopic(name=topic_item['name'],
                                num_partitions=topic_item['partitions'],
                                replication_factor=topic_item['replicas'])
            # Send create topic request
            r = kclient.create_topics([newTopic])
            # Save timing
            topic_item["time-to-create-sec"] = time.time() - create_start_s
            # Save errors if any for this single topic
            _errors = [e for e in getattr(r, 'topic_errors', []) if e[1] != 0]
            # No need to check topic name, we sent only one
            if len(_errors) > 0:
                topic_item['topic_errors'] = (_errors[1], _errors[2])
            return topic_item

        def _create_topic_batch(topic_item_list):
            # Create topic with time tracking
            create_start_s = time.time()
            specs = []
            for topic in topic_item_list:
                specs.append(
                    NewTopic(name=topic['name'],
                             num_partitions=topic['partitions'],
                             replication_factor=topic['replicas']))
            # Send whole batch to kafka client
            r = kclient.create_topics(specs)
            time_to_create_sec = time.time() - create_start_s
            # Filter topic errors if any
            _errors = [e for e in getattr(r, 'topic_errors', []) if e[1] != 0]
            # Transpose errors so names appear on 0, error code in 1 and Class in 2
            _errors = numpy.array(_errors).transpose()
            # Fill time and check errors
            for item in topic_item_list:
                item['time-to-create-sec'] = time_to_create_sec
                # if there is an error, create key
                if len(_errors) > 0 and item['name'] in _errors[0]:
                    idx = _errors[0].index(item['name'])
                    item['topic_errors'] = (_errors[1][idx], _errors[2][idx])
            return topic_item_list

        workers = 32
        # Proceed with topic name generation
        if self.skip_topic_names_randomization:
            topic_names = [
                f"{topic_name_prefix}-{i}" for i in range(target_count)
            ]
        else:
            # topic name len
            topic_name_length = self.name_length
            # Generate names
            # Account for variance of topic names based on length and available charset
            self.logger.info("Generating topic names using length of "
                             f"{topic_name_length} and prefix of "
                             f"{topic_name_prefix}")
            # Check that there is enough variations available
            random_str_len = topic_name_length - len(topic_name_prefix)
            charset_len = len(string.ascii_letters + string.digits)
            if random_str_len < 0:
                # prefix is longer than selected name len
                raise RuntimeError("Selected topic length "
                                   f"({topic_name_length}) is less than "
                                   f"prefix ({len(topic_name_prefix)})")
            elif charset_len**random_str_len < target_count:
                # Not enough space for randomizing in topic name
                raise RuntimeError(
                    f"Topic count ({target_count}) is more than "
                    "topic name randomization can deliver based "
                    f"on given length ({topic_name_length}) and "
                    f"prefix ('{topic_name_prefix}')")

            topic_names = []
            while len(topic_names) < target_count:
                # generate name with retry count of 20 if same name is generated
                new_name = self.generate_topic_name(topic_name_prefix,
                                                    topic_name_length)
                retries = 19
                while new_name in topic_names:
                    new_name = self.generate_topic_name(
                        topic_name_prefix, topic_name_length)
                    retries -= 1
                if retries < 0:
                    # Probability of this is higher if random_str_len < 3
                    # I.e. the longer the topic name and shorter the prefix,
                    # the less likely we'll hit the same name
                    raise RuntimeError(
                        "Failed to generate unique name after 20 retries")
                topic_names.append(new_name)

        # Topic creation
        self.logger.info(f"Creating {target_count} topics: "
                         f"batch size = {batch_size}, workers = {workers}")
        # Prepare topic desc templates
        _topic_item = {
            "index": 0,
            "batch-index": 0,
            "spec": None,
            "name": "topics-swarm-test",
            "partitions": 1,
            "replicas": 3,
            "kafka-response": None,
            "time-to-create-sec": 0,
        }
        timings = {
            "start_time_s": time.time(),
            "batch_timings_s": [],
        }
        # Topic creation
        topics = []
        count_created = 0
        while count_created < target_count:
            # Prepare next batch
            topic_batch = []
            remaining_count = target_count - count_created
            next_batch_size = remaining_count if remaining_count < batch_size else batch_size
            for i in range(next_batch_size):
                index = count_created + i
                _topic = deepcopy(_topic_item)
                # This is for code cleaniness
                # since the order will be differnt after ThreadPoolExecutor
                _topic['index'] = count_created + i
                # Since new batch is already started this is always +1
                _topic['batch-index'] = index // batch_size + 1
                _topic['name'] = topic_names[index]
                _topic['partitions'] = num_partitions
                _topic['replicas'] = num_replicas
                topic_batch.append(_topic)

            # Create batch
            batch_start_s = time.time()
            if use_kafka_batch:
                # Topic creation
                self.logger.info(f"Creating {target_count} topics using "
                                 f"batch size of {batch_size}")
                # If kafka native batching used, just send it all
                created_topics = _create_topic_batch(topic_batch)
                topics += created_topics
            else:
                # Use pool executor with worker count and send per-topic
                # creation requests
                self.logger.info(f"Creating {target_count} topics: "
                                 f"batch size = {batch_size}, "
                                 f"pool workers = {workers}")
                with ThreadPoolExecutor(max_workers=workers) as executor:
                    topic_index = count_created + 1
                    for result in executor.map(_create_single_topic,
                                               topic_batch):
                        # Set topic index based on order of creation
                        result['index'] = topic_index
                        topics.append(result)
                        topic_index += 1
            batch_creation_time_s = time.time() - batch_start_s

            # Peek creation timings
            timings['count_created'] = count_created
            timings['batch_timings_s'].append(batch_creation_time_s)
            # min/max
            creation_times = [t["time-to-create-sec"] for t in topics]
            timings["creation-time-max"] = max(creation_times)
            timings["creation-time-min"] = min(creation_times)
            timings['creation_times'] = creation_times

            # TODO: Add json output for ongoing data
            write_json(sys.stdout, {'timings': timings})
            # Exit on threshold > 5 min
            # I.e. single topic creation takes more than 5 min
            if timings["creation-time-max"] > 300:
                raise RuntimeError(
                    "Topic creation took too long during latest "
                    f"batch. Total created {count_created}")

            # Keep counting
            count_created += batch_size
        timings['end_time_s'] = time.time()
        return (topics, timings)


COMMAND_CREATE = 'create'
COMMAND_DELETE = 'delete'
commands = [COMMAND_CREATE, COMMAND_DELETE]


def main(args):

    tm = TopicSwarm(args.brokers,
                    args.workers,
                    args.issilent,
                    logger=setup_logger())

    if args.command == COMMAND_CREATE:
        try:
            tm.set_topic_config(args.name_length, args.skip_randomize)
            topics, timings = tm.create_many_topics(
                args.topic_count,
                topic_name_prefix=args.topic_prefix,
                use_kafka_batch=args.use_kafka_batch,
                batch_size=args.batch_size,
                num_partitions=args.num_partitions,
                num_replicas=args.num_replicas)

            data = {'topics': topics, 'timings': timings}
        except NoBrokersAvailable as e:
            data = {'error': f"{e.__str__()} for '{args.brokers}'"}
        except Exception as e:
            import traceback
            data = {
                'error':
                ''.join(traceback.format_exception(type(e), e,
                                                   e.__traceback__))
            }
        finally:
            write_json(sys.stdout, data)

    else:
        data = {
            'error':
            f"topic swarm command "
            f"'{args.command}' not yet implemented"
        }
        write_json(sys.stdout, data)
        sys.exit(1)

    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="TopicOperations")
    parser.add_argument('-b',
                        '--brokers',
                        dest="brokers",
                        default='localhost:9092',
                        help="Bootstrap broker(s) (host[:port])")

    parser.add_argument('-s',
                        '--silent',
                        dest="issilent",
                        default=True,
                        type=bool,
                        help="Silent mode with no ongoing output")

    parser.add_argument('-w',
                        '--workers',
                        dest="workers",
                        default=32,
                        type=int,
                        help="Parallel processes working on single batch "
                        "when per-topic operation requests used.")

    parser.add_argument('-bs',
                        '--batch-size',
                        dest="batch_size",
                        default=512,
                        type=int,
                        help="Number of topics in one batch.")

    subparsers = parser.add_subparsers(dest='command', required=True)
    parser_create = subparsers.add_parser("create")
    parser_create.add_argument(
        "-f",
        "--topic-prefix",
        dest="topic_prefix",
        default="topics-swarm-test",
        help="Topic prefix to use when creating. "
        "Formats: '<prefix>-p<partitions>-r<replicas>-<randomized>' or <prefix>-p<partitions>-r<replicas>-<sequence_number>"
    )
    parser_create.add_argument('-c',
                               '--topic-count',
                               dest="topic_count",
                               default=100,
                               type=int,
                               help="Number of topics to create")
    parser_create.add_argument('-k',
                               '--kafka-batching',
                               dest='use_kafka_batch',
                               action='store_true',
                               default=True,
                               help="Put whole batch in kafka client instead "
                               "of per-topic operation")
    parser_create.add_argument('-l',
                               '--topic-name-length',
                               dest="name_length",
                               default=200,
                               type=int,
                               help="Single topic name length")

    parser_create.add_argument('--skip-randomize-names',
                               dest="skip_randomize",
                               action="store_true",
                               default=False,
                               help="Do not randomize topic names")

    parser_create.add_argument('-p',
                               '--partitions',
                               dest="num_partitions",
                               default=1,
                               type=int,
                               help="Number of partitions in each topic")

    parser_create.add_argument('-r',
                               '--replicas',
                               dest="num_replicas",
                               default=3,
                               type=int,
                               help="Number of replicas in each topic")

    parser_delete = subparsers.add_parser("delete")

    main(parser.parse_args())
