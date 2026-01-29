from dataclasses import dataclass

import pandas as pd
from typing import Any


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
    message_period: str
    topics_per_client: int

    @property
    def topic_name_prefix(self):
        return f"{self.profile_name}-p{self.num_partitions}-r{self.num_replicas}"

    def total_running_time(self) -> float:
        mp = pd.Timedelta(self.message_period)
        return (mp * self.message_count).total_seconds()

    def message_rate(self) -> float:
        mp = pd.Timedelta(self.message_period)
        return 1 / mp.total_seconds()


class ProfileDefinitions:
    topic_profile_t40k_p1 = {
        "topic_count": 39_996,
        "batch_size": 4096,
        "topic_name_length": 200,
        "num_partitions": 1,
        "num_replicas": 3,
        "use_kafka_batching": True,
        "profile_name": "topic-scale-t40k-p1",
        "message_count": 6 * 60,  # 6 mins
        "message_period": "1s",
        "topics_per_client": 22,
    }


class TopicScaleProfileManager:
    def __init__(self):
        self.profiles = ProfileDefinitions()

    def _list_profiles(self):
        return [m for m in dir(self.profiles) if not m.startswith("__")]

    def _load_profile_data(self, profile_name: str):
        _profile_data = getattr(self.profiles, profile_name, None)
        if _profile_data is None:
            raise RuntimeError(
                f"Profile '{profile_name}' is not found among: "
                f"{', '.join(self._list_profiles())}"
            )
        else:
            return _profile_data

    def get_profile(self, profile_name: str = "default"):
        return TopicScaleTestProfile(**self._load_profile_data(profile_name))

    def get_custom_profile(self, base_profile_name: str, data: dict[str, Any]):
        _profile_data = self._load_profile_data(base_profile_name)
        _profile_data.update(data)
        try:
            _profile = TopicScaleTestProfile(**_profile_data)
        except Exception as e:
            # rethrow Exception with good message
            raise RuntimeError(
                "Invalid custom data provided "
                f"for base profile of '{base_profile_name}'"
            ) from e
        return _profile
