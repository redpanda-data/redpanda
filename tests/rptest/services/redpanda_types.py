import collections
from dataclasses import astuple, dataclass
from typing import Iterator

# Basic types shared between various redpanda service and test modes.


@dataclass
class SaslCredentials:
    """Credentials and algorithm for SASL authentication."""

    username: str
    """The username for SASL authentication."""

    password: str
    """The password for SASL authentication."""

    algorithm: str
    """The SASL authentication mechanism."""

    # allow credentials to be unpacked for backwards compat
    def __iter__(self) -> Iterator[str]:
        return iter(astuple(self))
