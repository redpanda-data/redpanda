from dataclasses import dataclass


@dataclass(kw_only=True)
class TopicScaleTestProfile:
    # Default values are set minimal scale run on i3en.xlarge
    # Parameters
    topic_count: int
    batch_size: int
    topic_name_length: int
    num_partitions: int
    num_replicas: int
    use_kafka_batching: bool
    profile_name: str
    message_count: int
    messages_per_second_per_producer: int

    @property
    def topic_name_prefix(self):
        return f"{self.profile_name}-" \
               f"p{self.num_partitions}-r{self.num_replicas}"


class ProfileDefinitions():
    # Minimal load
    # 2 vcpus, 1 msg/sec, min batch sizing
    default = {
        "topic_count": 6000,
        "batch_size": 512,
        "topic_name_length": 128,
        "num_partitions": 1,
        "num_replicas": 3,
        "use_kafka_batching": True,
        "profile_name": "topic-scale-default",
        "message_count": 100,
        "messages_per_second_per_producer": 1
    }
    topic_profile_t10k_p1 = {
        "topic_count": 10000,
        "batch_size": 2048,
        "topic_name_length": 200,
        "num_partitions": 1,
        "num_replicas": 3,
        "use_kafka_batching": True,
        "profile_name": "topic-scale-t10k_p1",
        "message_count": 1000,
        "messages_per_second_per_producer": 1
    }
    topic_profile_t10k_p4 = {
        "topic_count": 10000,
        "batch_size": 2048,
        "topic_name_length": 200,
        "num_partitions": 4,
        "num_replicas": 3,
        "use_kafka_batching": True,
        "profile_name": "topic-scale-t10k-p4",
        "message_count": 1000,
        "messages_per_second_per_producer": 1
    }
    topic_profile_t1_p40k = {
        "topic_count": 1,
        "batch_size": 2048,
        "topic_name_length": 200,
        "num_partitions": 40000,
        "num_replicas": 3,
        "use_kafka_batching": True,
        "profile_name": "topic-scale-t1-p40k",
        "message_count": 1000,
        "messages_per_second_per_producer": 1
    }
    topic_profile_t40k_p1 = {
        "topic_count": 39_996,
        "batch_size": 4096,
        "topic_name_length": 200,
        "num_partitions": 1,
        "num_replicas": 3,
        "use_kafka_batching": True,
        "profile_name": "topic-scale-t40k-p1",
        "message_count": 1000,
        "messages_per_second_per_producer": 1
    }


class TopicScaleProfileManager():
    def __init__(self):
        self.profiles = ProfileDefinitions()

    def _list_profiles(self):
        return [m for m in dir(self.profiles) if not m.startswith("__")]

    def _load_profile_data(self, profile_name):
        _profile_data = getattr(self.profiles, profile_name, None)
        if _profile_data is None:
            raise RuntimeError(f"Profile '{profile_name}' is not found among: "
                               f"{', '.join(self._list_profiles())}")
        else:
            return _profile_data

    def get_profile(self, profile_name="default"):
        return TopicScaleTestProfile(**self._load_profile_data(profile_name))

    def get_custom_profile(self, base_profile_name, data):
        _profile_data = self._load_profile_data(base_profile_name)
        _profile_data.update(data)
        try:
            _profile = TopicScaleTestProfile(**_profile_data)
        except Exception as e:
            # rethrow Exception with good message
            raise RuntimeError(
                "Invalid custom data provided "
                f"for base profile of '{base_profile_name}'") from e
        return _profile
