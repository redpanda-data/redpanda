from copy import copy
from dataclasses import astuple, dataclass
from enum import Enum, auto
from logging import Logger
import re
from typing import Iterator, Protocol, Sequence
from rptest.utils.allow_logs_on_predicate import AllowLogsOnPredicate

# pyright: strict

# Basic types shared between various redpanda service and test modes.

# Things which are allowed in the low_allow_list for @cluster
LogAllowListElem = str | AllowLogsOnPredicate | re.Pattern[str]
LogAllowList = Sequence[LogAllowListElem]


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

    # allow credentials to be subscripted for backwards compat
    def __getitem__(self, index: int) -> str:
        return list(self)[index]

    @property
    def mechanism(self):
        """The SASL authentication mechanism (an alias for self.algorithm)."""
        return self.algorithm


class SecurityProtocol(Enum):
    """The four possible security protocol options for Kafka authentication."""
    PLAINTEXT = auto()
    """SASL and TLS disabled."""
    SSL = auto()
    """SASL disabled, TLS enabled."""
    SASL_PLAINTEXT = auto()
    """SASL enabled, TLS disabled."""
    SASL_SSL = auto()
    """SASL and TLS enabled."""

    # Use the base name so we can use the formatted protocol directly
    # in format contexts like f-strings.
    def __str__(self):
        return self.name


SIMPLE_SASL_MECHANISMS = ['SCRAM-SHA-256', 'SCRAM-SHA-512']
COMPLEX_SASL_MECHANISMS = ['GSSAPI', 'OAUTHBEARER']


class InvalidKafkaSecurity(RuntimeError):
    """Indicates a consistency check failed while creating or manipulating security
    credentials. This is a local check and does not involve contacting the server."""
    pass


class NonSASL:
    """Empty indicator class for KafkaClientSecurity objects that do no have SASL enabled."""
    pass


@dataclass
class ComplexSASL:
    """A class indicating that SASL is enabled, but it is not a simple mechanism
    (e.g., SCRAM, PLAIN) that be be represented by SaslCredentials. Client wrappers
    will generally need to switch on the mechanism and handle them specifically."""
    mechanism: str


def check_username_password(username: str | None, password: str | None):
    """Check that either both username and password are set, or neither.
    Empty string username is considered unset, but empty password is considered set"""
    if username and password is None:
        raise InvalidKafkaSecurity('username set but password not set')
    if password is not None and not username:
        raise InvalidKafkaSecurity('password set but username not set')


class KafkaClientSecurity:
    """A class that bundles up the security information necessary for a client to connect to
    a broker."""
    def __init__(self, simple_sasl: SaslCredentials | None, tls_enabled: bool):
        if not simple_sasl:
            self._sasl_credentials = NonSASL()
        else:
            assert isinstance(simple_sasl, SaslCredentials)
            self._sasl_credentials = simple_sasl
        self.tls_enabled = tls_enabled

    _sasl_credentials: NonSASL | SaslCredentials | ComplexSASL

    tls_enabled: bool
    """True iff the Kafka listener has TLS enabled."""

    #

    @property
    def sasl_enabled(self):
        """Returns true if SASL is enabled, either simple or complex."""
        return not isinstance(self._sasl_credentials, NonSASL)

    @property
    def is_simple(self):
        """Determines if the contained authentication method is "simple". An instance is simple
        if SASL is disabled or if is enabled and is simple SASL mechanisms: i.e., those which can be handled
        by generated configuration properties. Non-simple mechanisms (like OAUTH) require additional
        handling by the client (e.g., registering an OAUTH callback) and this class throws in methods
        which are documented as requiring simple configuration)."""
        return isinstance(self._sasl_credentials, NonSASL) or isinstance(
            self._sasl_credentials, SaslCredentials)

    @property
    def security_protocol(self):
        lookup: dict[tuple[bool, bool], SecurityProtocol] = {
            (False, False): SecurityProtocol.PLAINTEXT,
            (False, True): SecurityProtocol.SASL_PLAINTEXT,
            (True, False): SecurityProtocol.SSL,
            (True, True): SecurityProtocol.SASL_SSL,
        }

        return lookup[(self.tls_enabled, self.sasl_enabled)]

    @property
    def username(self) -> str | None:
        """The SASL username or None if SASL is not enabled or the SASL mechanism does not use a username."""
        return self._sasl_credentials.username if isinstance(
            self._sasl_credentials, SaslCredentials) else None

    @property
    def password(self) -> str | None:
        """The SASL password or None if SASL is not enabled or the SASL mechanism does not use a password."""
        return self._sasl_credentials.password if isinstance(
            self._sasl_credentials, SaslCredentials) else None

    @property
    def mechanism(self) -> str | None:
        """The SASL mechanism or None if SASL is not enabled."""
        return None if isinstance(
            self._sasl_credentials,
            NonSASL) else self._sasl_credentials.mechanism

    def override(self, username: str | None, password: str | None,
                 sasl_mechanism: str | None, tls_enabled: bool | None):
        """Return a new object with zero or more overridden values passed as
           arguments to this function.

           An override occurs if the argument is non-None (and not empty string
           for username and mechanism). Only certain override configurations
           are allowed:

           Overriding both the username and password.
           Overriding all of the username, password and sasl mechanism (mechanism must be simple).
           Overriding only the sasl mechanism (mechanism must be non-simple and username and password are
           cleared after this override).

           In addition the tls_enabled value always may be overridden independently of
           the above rules.
        """

        ret = copy(self)

        check_username_password(username, password)

        if sasl_mechanism and not username:
            if sasl_mechanism in SIMPLE_SASL_MECHANISMS:
                raise InvalidKafkaSecurity(
                    'overriding mechanism without updating user/pass')
            assert sasl_mechanism in COMPLEX_SASL_MECHANISMS, f'unknown SASL mechanism: {sasl_mechanism}'
            ret._sasl_credentials = ComplexSASL(mechanism=sasl_mechanism)

        if username and password:
            if not sasl_mechanism:
                if not isinstance(ret._sasl_credentials, SaslCredentials):
                    raise InvalidKafkaSecurity(
                        f'overriding user/pass but existing credentials are not simple SASL: {ret._sasl_credentials}'
                    )
                else:
                    sasl_mechanism = ret._sasl_credentials.mechanism

            ret._sasl_credentials = SaslCredentials(username, password,
                                                    sasl_mechanism)

        if tls_enabled is not None:
            ret.tls_enabled = tls_enabled

        return ret

    def to_dict(self) -> dict[str, str | int]:
        """Return the security configuration as a dictionary suitable for use by kafka-python.

        This supports only SASL disabled (returns an empty dict) or simple SASL (SCRAM) mechanisms
        and throws otherwise.

        This is here for some existing use cases but try not to use this.
        """

        if (c := self.simple_credentials()):
            return dict(security_protocol=self.security_protocol.name,
                        sasl_mechanism=c.mechanism,
                        sasl_plain_username=c.username,
                        sasl_plain_password=c.password,
                        request_timeout_ms=30000,
                        api_version_auto_timeout_ms=3000)
        else:
            # by convention we return an empty dict when we are using PLAINTEXT
            # since clients default to this protocol and so don't require an
            # explicit security protocol configuration in that case
            return dict(security_protocol=self.security_protocol.name
                        ) if self.tls_enabled else {}

    def simple_credentials(self) -> SaslCredentials | None:
        """Return SaslCredentials for this configuration if possible.

        Throws an exception if auth is not simple, otherwise returns SaslCredentials if SASL is enabled
        or None if SASL is disabled."""
        if not self.is_simple:
            raise RuntimeError(
                f'Calling method requires simple security, but this is not simple: {self}'
            )
        c = self._sasl_credentials
        return c if isinstance(c, SaslCredentials) else None


# A KafkaClientSecurity constant presenting "PLAINTEXT" security,
# i.e., no SASL and no TLS.
PLAINTEXT_SECURITY = KafkaClientSecurity(None, False)

# A KafkaClientSecurity constant presenting "SSL" security,
# i.e., no SASL and TLS enabled.
SSL_SECURITY = KafkaClientSecurity(None, True)


class RedpandaServiceForClients(Protocol):
    """A protocol encoding some basic functionality of RedpandaService and similar
    Service classes (like RedpandaCloudService), such that clients that take a
    redpanda service object can be property typed, without depending on the full
    class definitions (which might introduce a circular dependency in the typing
    and which is otherwise undesirable since it would make the tool classes
    more tightly tied to the service.

    Method documentation lives on the service implementations themselves and is
    not repeated here."""

    logger: Logger

    def kafka_client_security(self) -> KafkaClientSecurity:
        ...

    def brokers(self) -> str:
        ...
