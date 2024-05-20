from typing import List, Optional, TypedDict, Union

import pyarrow as pa
import ray

from krayon.join._table_filter_utils import (
    TableFilterPair,
    make_empty_table_filter_info,
)


class InnerJoinArgs(TypedDict):
    keys: Union[str, List[str]]
    right_keys: Union[str, List[str], None]
    left_suffix: Optional[str]
    right_suffix: Optional[str]
    coalesce_keys: bool
    use_threads: bool


@ray.remote(num_returns=2)
def inner_join_tables(
    left: pa.Table,
    right: pa.Table,
    *,
    inner_join_args: InnerJoinArgs,
    schema_table: Optional[pa.Table] = None,
    result_table_index: int,
    result_label: int = 0,
) -> TableFilterPair:
    result = left.join(
        right,
        join_type="inner",
        **inner_join_args,
    )

    if schema_table is not None:
        result = pa.concat_tables([schema_table, result], promote=True)

    return result, make_empty_table_filter_info(
        table=result, index=result_table_index, label=result_label
    )
