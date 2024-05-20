from typing import Any, Generator, Iterable, List, Optional

import ray
from ray import ObjectRef


def wait_all(
    object_refs: Iterable[ObjectRef],
    batch_size: int = 1,
    iteration_timeout_seconds: Optional[float] = None,
) -> Generator[List[Any], None, None]:
    """
    On each iteration, blocks until, out of the given `object_refs`,
    at least `num_returns` are ready, or `iteration_timeout_seconds` have passed.
    Then, yields a `List` containing the contents of
    any of the given `object_refs` that are ready,
    with a maximum length of `num_returns`.

    No guarantees are made about overall yield order,
    but for each iteration, the elements within the yielded `List`
    will be in the order initially provided in `object_refs`.

    Implements recommendation for avoiding this anti-pattern:
    https://docs.ray.io/en/latest/ray-core/patterns/ray-get-submission-order.html#code-example

    NOTE:
    Consumes entire `object_refs` (and stores all `ObjectRef`s in memory)
    before starting to yield any indices.

    Args:
        object_refs:
            `ObjectRef`s to wait on, which may or may not be ready.
        batch_size:
            Maximum length of each yielded `List`.
        iteration_timeout_seconds:
            The maximum amount of time in seconds to wait before yielding
            on each iteration.

    Yields:
        `List` containing the contents of
        any of the given `object_refs` that are ready,
        with a maximum length of `batch_size`.
    """
    remainder = list(object_refs)
    while remainder:
        done, remainder = ray.wait(
            remainder,
            num_returns=min(batch_size, len(remainder)),
            timeout=iteration_timeout_seconds,
        )
        yield ray.get(done)
