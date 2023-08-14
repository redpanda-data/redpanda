class GCPClient:
    """
    GCP Client class.
    Should implement function similar to AWS EC2 client
    """
    def __init__(self) -> None:
        self._cli = self._make_client()

    def _make_client(self):
        return None
