from copy import copy
from dataclasses import astuple, dataclass
from enum import Enum, auto
from typing import Iterator

# pyright: strict

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

    # allow credentials to be subscripted for backwards compat
    def __getitem__(self, index: int) -> str:
        return list(self)[index]

    @property
    def mechanism(self):
        """The SASL mechanism (an alias for self.algorithm)."""
        return self.algorithm


class SecurityProtocol(Enum):
    PLAINTEXT = auto()
    SSL = auto()
    SASL_PLAINTEXT = auto()
    SASL_SSL = auto()

    def __str__(self):
        return self.name


SIMPLE_SASL_MECHANISMS = ['SCRAM-SHA-256', 'SCRAM-SHA-512']
COMPLEX_SASL_MECHANISMS = ['GSSAPI', 'OAUTHBEARER']


class InvalidKafkaSecurity(RuntimeError):
    pass


class NonSASL:
    pass


@dataclass
class ComplexSASL:
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
        """Determines if an the contained authentication method is "simple". An instance is simple
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
    def username(self):
        """The SASL username or None if SASL is not enabled or the SASL mechanism does not use a username."""
        return self._sasl_credentials.username if isinstance(
            self._sasl_credentials, SaslCredentials) else None

    @property
    def password(self):
        """The SASL password or None if SASL is not enabled or the SASL mechanism does not use a password."""
        return self._sasl_credentials.password if isinstance(
            self._sasl_credentials, SaslCredentials) else None

    @property
    def mechanism(self):
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
           Overriding all of the username, password and sasl configuration.

           The tls_enabled value always may be overridden independently of
           the above rules.
        """

        ret = copy(self)
        if username and password is None:
            raise InvalidKafkaSecurity('username set but password not set')
        if password is not None and not username:
            raise InvalidKafkaSecurity('password set but username not set')

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

    def override_tls(self, tls_enabled: bool | None):
        """Return a new object which is a copy of this one, but with its
        tls_enabled value overridden by the argument if it is non-None.
        """
        ret = copy(self)
        if tls_enabled is not None:
            ret.tls_enabled = tls_enabled
        return ret

    def _require_simple(self):
        """Check that auth is simple, otherwise throw then returns SaslCredentials if SASL is enabled
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
# i.e., SASL and TLS enabled.
SSL_SECURITY = KafkaClientSecurity(None, True)
