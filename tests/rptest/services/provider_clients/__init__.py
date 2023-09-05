from rptest.services.provider_clients.ec2_client import EC2Client
from rptest.services.provider_clients.gcp_client import GCPClient


def make_provider_client(provider, logger, region, key, secret):
    _client = None
    if provider == 'AWS':
        _client = EC2Client(region, key, secret, logger)
    elif provider == 'GCP':
        # In scope of GCP, key contains path to keyfile
        _client = GCPClient(region, key, logger)
    return _client
