"""Miscellaneous helper functions and type definitions."""

from typing import Collection, Iterator, List, Literal, Tuple, TypeVar, Union

import pyarrow as pa
import ray
from ray import ObjectRef

T = TypeVar("T")

ObjectOrRef = Union[T, ObjectRef]  # specifically Union[T, ObjectRef[T]]
TableOrRef = ObjectOrRef[pa.Table]

JoinType = Literal[
    "inner",
    "left semi",
    "right semi",
    "left anti",
    "right anti",
    "left outer",
    "right outer",
    "full outer",
]
ALLOWED_JOIN_TYPES = set(JoinType.__args__)


# class SideArgs(TypedDict):
#     keys: Union[str, List[str]]
#     right_keys: Union[str, List[str], None]
#     left_suffix: Optional[str]
#     right_suffix: Optional[str]


def ensure_object_ref(target: ObjectOrRef[T]) -> ObjectRef:
    """
    Returns the input `target` if it is already an instance of `ObjectRef`,
    otherwise returns an `ObjectRef[T]` containing the input `target`.
    """
    return target if isinstance(target, ObjectRef) else ray.put(target)


def validate_join_keys(
    left_column_names: List[str],
    right_column_names: List[str],
    keys: Union[str, List[str]],
    right_keys: Union[str, List[str], None] = None,
) -> None:
    """
    Validates the join keys.
    TODO: docstring, implementation
    """
    ...
    # if join_type not in ALLOWED_JOIN_TYPES:
    #     raise TypeError(
    #         f"invalid join_type {repr(join_type)}"
    #         f" - must be one of the following: {ALLOWED_JOIN_TYPES}"
    #     )
    # if right_keys is not None:
    #     if isinstance(keys, str):
    #         if not (isinstance(right_keys, str) or len(right_keys == 1)):
    #             raise ValueError("mismatch between keys and right_keys")
    #     elif len(keys) != len(right_keys):
    #         raise ValueError("mismatch between keys and right_keys")


def make_key_pairs(
    keys: Union[str, Collection[str]],
    right_keys: Union[str, Collection[str], None] = None,
) -> Iterator[Tuple[str, str]]:
    """
    Standardizes the format of keys into `(left_key, right_key)` tuples.
    The number of pairs will be the same as the number of keys.

    If `right_keys is not None`, there must be a matching number
    of `keys` and `right_keys`; raises a `ValueError` otherwise.
    """
    key_collection = [keys] if isinstance(keys, str) else keys
    if right_keys is None:
        return [(key, key) for key in key_collection]

    right_key_collection = [right_keys] if isinstance(right_keys, str) else right_keys
    num_keys = len(key_collection)
    num_right_keys = len(right_key_collection)
    if num_keys == num_right_keys:
        return zip(key_collection, right_key_collection)
    else:
        raise ValueError(
            f"Mismatch between number of keys {num_keys} "
            f"and right_keys {num_right_keys}"
        )


def intersect_tables(
    left: pa.Table, right: pa.Table, use_threads: bool = True
) -> pa.Table:
    """
    Assumes `left` and `right` have the same schema.
    """
    return left.join(
        join_type="left semi",
        right_table=right,
        keys=left.column_names,
        use_threads=use_threads,
    )


def swap_side_keys(
    keys: Union[str, List[str]], right_keys: Union[str, List[str], None] = None,
) -> Tuple[Union[str, List[str]], Union[str, List[str], None]]:
    right_keys_present = right_keys is not None
    return (
        right_keys if right_keys_present else keys,
        keys if right_keys_present else None,
    )


# def swap_sides(
#     *,
#     keys: Union[str, List[str]],
#     right_keys: Union[str, List[str], None] = None,
#     left_suffix: Optional[str],
#     right_suffix: Optional[str],
# ) -> SideArgs:
#     right_keys_present = right_keys is not None
#     return dict(
#         keys=right_keys if right_keys_present else keys,
#         right_keys=keys if right_keys_present else None,
#         left_suffix=right_suffix,
#         right_suffix=left_suffix,
#     )


# def optional_union(top: Optional[Dataset], bottom: Optional[Dataset]) -> Optional[Dataset]:
#     if top is None:
#         return bottom
#     elif bottom is None:
#         return top
#     else:
#         return top.union(bottom)
