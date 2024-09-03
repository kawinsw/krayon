from dataclasses import dataclass
from typing import Callable, Generator, Iterable, Tuple, TypeVar

import ray
from ray import ObjectRef

from krayon.join.utils import ObjectOrRef
from krayon._wait_all import wait_all


T = TypeVar("T")
FilterFnType = Callable[[T], bool]


@dataclass
class FilterInfo:
    """
    Generic dataclass containing relevant details
    about the contents of some `ObjectRef[T]` being filtered
    that we want to access without having to access the entire `T`.
    """
    keep: bool
    index: int
    label: int = 0


def make_filter_info(
    obj: T, filter_fn: FilterFnType[T], index: int, label: int = 0
) -> FilterInfo:
    """
    Factory function instantiating a `FilterInfo` from the given `obj`.
    """
    return FilterInfo(keep=filter_fn(obj), index=index, label=label)


make_filter_info_remote = ray.remote(make_filter_info)


# def make_all_filter_info(
#     targets: Iterable[ObjectOrRef[T]],
#     start_index: int,
#     filter_fn: ObjectOrRef[FilterFnType[T]],
#     label: int = 0,
# ) -> List[ObjectRef[FilterInfo]]:
#     """
#     Instantiates a `FilterInfo` for each of the given `targets`.
#     TODO: Convert to `Generator`?
#     """
#     return [
#         make_filter_info_remote.remote(
#             obj=target, index=i, filter_fn=filter_fn, label=label
#         )
#         for i, target in enumerate(targets, start=start_index)
#     ]


def make_all_filter_info_in_groups(
    groups: Iterable[Iterable[ObjectOrRef[T]]],
    filter_fn: ObjectOrRef[FilterFnType[T]],
) -> Generator[Tuple[ObjectOrRef[T], ObjectRef], None, None]:
    """
    For each `elem` in each of the given `groups`, yields `(elem, filter_info_ref)`,
    where `filter_info_ref` is an `ObjectRef[FilterInfo]`
    containing a `FilterInfo` corresponding to `elem`, based on the given `filter_fn`.
    """
    count = 0
    for i, group in enumerate(groups):
        for elem in group:
            yield elem, make_filter_info_remote.remote(
                obj=elem, filter_fn=filter_fn, index=count, label=i
            )
            count += 1


def filter_refs_in_groups(
    groups: Iterable[Iterable[ObjectOrRef[T]]],
    filter_fn: ObjectOrRef[FilterFnType[T]],
) -> Generator[Tuple[ObjectOrRef[T], int], None, None]:
    filter_ref_pairs = list(make_all_filter_info_in_groups(groups, filter_fn))
    for filter_info in wait_and_filter(
        filter_info_ref for _, filter_info_ref in filter_ref_pairs
    ):
        yield filter_ref_pairs[filter_info.index][0], filter_info.label


# def filter_pairs(
#     filter_ref_pairs: Sequence[FilterRefPair]
# ) -> Generator[ObjectRef[pa.Table], None, None]:
#     """
#     On each iteration, blocks until any given `FilterInfo` is ready.
#     Then, for a ready `FilterInfo`, if `FilterInfo.keep`,
#     yields the corresponding `Table`.
#     No guarantees made about yield order.
#
#     NOTE:
#     Assumes each `FilterInfo.index` corresponds to
#     its index within the input `filter_ref_pairs`.
#     """
#     for index in filter_indices(filter_info for _, filter_info in filter_ref_pairs):
#         yield filter_ref_pairs[index][0]


def wait_and_filter(
    filter_info_refs: Iterable[ObjectRef]  # specifically ObjectRef[FilterInfo]
) -> Generator[FilterInfo, None, None]:
    """
    On each iteration, blocks until any given `FilterInfo` is ready.
    Then, for a ready `FilterInfo`, if `FilterInfo.keep`, yields it.
    No guarantees made about yield order.

    NOTE:
    Consumes entire `filter_info_refs` (and stores all `ObjectRef`s in memory)
    before starting to yield.
    """
    for (filter_info,) in wait_all(filter_info_refs, batch_size=1):
        if filter_info.keep:
            yield filter_info
    # remainder = list(filter_info_refs)
    # while remainder:
    #     (completed_filter_info_ref,), remainder = ray.wait(remainder, num_returns=1)
    #     filter_info = ray.get(completed_filter_info_ref)
    #     if filter_info.keep:
    #         yield filter_info


# def _cumulative_sum(nums: Iterable[int]) -> Generator[int, None, None]:
#     total = 0
#     for num in nums:
#         total += num
#         yield total
