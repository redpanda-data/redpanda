import os


def get_cloud_provider() -> str:
    """
    Returns the cloud provider in use.  If one is not set then return 'docker'
    """
    return os.getenv("CLOUD_PROVIDER", "docker")
