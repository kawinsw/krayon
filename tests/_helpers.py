"""Internal utils for testing."""

from collections.abc import Hashable
from typing import Callable, Counter, List, Tuple, TypeVar

import pyarrow as pa
import ray
from ray.data import Dataset

from krayon.utils import get_values_in_tuple

T = TypeVar("T")
RecordCounterMaker = Callable[[T, List[str]], Counter[Tuple[Hashable, ...]]]

# def tables_have_same_records(left: pa.Table, right: pa.Table) -> bool:
#     """Assumes all values in both `left` and `right` are hashable."""
#     if not left.schema.equals(right.schema):
#         return False
#     else:
#         column_names = left.column_names
#         left_record_counter = table_to_record_counter(left, column_names)
#         right_record_counter = table_to_record_counter(right, column_names)
#         return left_record_counter == right_record_counter


# @pytest.fixture(scope="module")
# def table_to_record_counter() -> RecordCounterMaker[pa.Table]:
#     return _table_to_record_counter_fn


# @pytest.fixture(scope="module")
# def ds_to_record_counter() -> RecordCounterMaker[Dataset]:
#     return _ds_to_record_counter_fn


def table_to_record_counter(
    table: pa.Table, column_names: List[str]
) -> Counter[Tuple[Hashable, ...]]:
    """Assumes all values in the input `table` are hashable."""
    return Counter(
        get_values_in_tuple(record, column_names)
        for record in table.to_pylist()
    )


_table_to_record_counter_ray_fn = ray.remote(table_to_record_counter)


def ds_to_record_counter(
    ds: Dataset, column_names: List[str]
) -> Counter[Tuple[Hashable, ...]]:
    """
    Assumes all values in the input `ds` are hashable.
    Not intended to scale over large numbers of distinct rows.
    """
    table_record_counter_refs = [
        _table_to_record_counter_ray_fn.remote(table_ref, column_names)
        for table_ref in ds.to_arrow_refs()
    ]
    ds_record_counter = Counter()
    for table_record_counter in ray.get(table_record_counter_refs):
        ds_record_counter.update(table_record_counter)
    return ds_record_counter
