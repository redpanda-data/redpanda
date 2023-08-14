from rptest.services.provider_clients.ec2_client import EC2Client
from rptest.services.provider_clients.gcp_client import GCPClient


def make_provider_client(provider, provider_config=None):
    _client = None
    if provider == 'AWS':
        _client = EC2Client(provider_config)
    elif provider == 'GCP':
        _client = GCPClient(provider_config)
    return _client
