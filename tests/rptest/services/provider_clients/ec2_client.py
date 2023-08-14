import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


class EC2Client:
    """
    This almost mimics S3Client from archival.
    """
    def __init__(self, config, logger, endpoint=None, disable_ssl=True):

        self._region = config['region']
        self._access_key = config['access_key']
        self._secret_key = config['secret_key']
        self._endpoint = endpoint
        self._disable_ssl = disable_ssl
        self._cli = self._make_client()
        self.logger = logger

        logger.debug(f"Created EC2 Client: {self._region}, {endpoint}, "
                     f"key is set = {self._access_key is not None}")

    def _make_client(self):
        cfg = Config(region_name=self._region,
                     retries={
                         'max_attempts': 10,
                         'mode': 'adaptive'
                     })
        return boto3.client('ec2',
                            config=cfg,
                            aws_access_key_id=self._access_key,
                            aws_secret_access_key=self._secret_key,
                            endpoint_url=self._endpoint,
                            use_ssl=not self._disable_ssl)
