from typing import Tuple

import pyarrow as pa
import ray
from ray import ObjectRef

from krayon.join._filter_utils import FilterInfo, make_filter_info


TableFilterPair = Tuple[pa.Table, FilterInfo]
TableFilterRefPair = Tuple[ObjectRef, ObjectRef]
# Specifically, TableFilterRefPair = Tuple[ObjectRef[pa.Table], ObjectRef[FilterInfo]]


def make_empty_table_filter_info(
    table: pa.Table, index: int, label: int = 0
) -> FilterInfo:
    return make_filter_info(
        obj=table, filter_fn=table_is_not_empty, index=index, label=label
    )


# def filter_table_refs(ref_pairs: Sequence[TableFilterRefPair]) -> List[pa.Table]:
#     """
#     Assumes that each `FilterInfo.index` is equal to
#     the index of the corresponding pair in `ref_pairs`.

#     Blocks and materializes all contents in `ref_pairs` (in Ray object store).
#     """
#     return [
#         ref_pairs[filter_info.index][0]
#         for filter_info in wait_and_filter(filter_info for _, filter_info in ref_pairs)
#     ]
    # return (
    #     ray.data.from_arrow_refs(table_refs)
    #     if len(table_refs) > 0
    #     else ray.data.from_arrow([schema_table])
    # )


def table_is_not_empty(table: pa.Table) -> bool:
    """Returns whether the given `table` has non-zero rows."""
    return table.num_rows != 0


table_is_not_empty_ref = ray.put(table_is_not_empty)
