from _typeshed import Incomplete

class HttpMixin:
    def http_request(
        self,
        url,
        method,
        data: str = ...,
        headers: Incomplete | None = ...,
        timeout: Incomplete | None = ...,
    ): ...
