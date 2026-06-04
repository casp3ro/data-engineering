from typing import Iterator, Protocol, runtime_checkable


@runtime_checkable
class IListingProducer(Protocol):
    def send_batch(self, listings: Iterator[dict]) -> int: ...
