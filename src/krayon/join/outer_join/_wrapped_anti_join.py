from dataclasses import dataclass
from typing import Iterable, List, Union

import pyarrow as pa
import ray
from ray.data import Dataset

from krayon.join._table_filter_utils import (
    TableFilterPair,
    TableFilterRefPair,
    make_empty_table_filter_info,
)

from krayon.join.left_anti_join import table_left_anti_join_dataset
from krayon.join.right_anti_join import dataset_right_anti_join_table


@dataclass
class _WrappedAntiJoinSharedArgs:
    keys: Union[str, List[str]]
    right_keys: Union[str, List[str], None] = None
    schema_table: pa.Table
    result_label: int = 0
    use_threads: bool = True


def tables_left_anti_join_dataset(
    left: Iterable[pa.Table],
    right: Dataset,
    *,
    keys: Union[str, List[str]],
    right_keys: Union[str, List[str], None] = None,
    schema_table: pa.Table,
    # start_result_table_index: int = 0,
    result_label: int = 0,
    use_threads: bool = True,
) -> List[TableFilterRefPair]:
    shared_args = _WrappedAntiJoinSharedArgs(
        keys=keys,
        right_keys=right_keys,
        schema_table=schema_table,
        result_label=result_label,
        use_threads=use_threads,
    )
    shared_args_ref = ray.put(shared_args)

    return [
        _wrapped_table_left_anti_join_dataset.remote(
            left=left_table,
            right=right,
            shared_args=shared_args_ref,
            # keys=keys,
            # right_keys=right_keys,
            # schema_table=schema_table,
            result_table_index=i,
            # result_label=result_label,
            # use_threads=use_threads,
        )
        for i, left_table in enumerate(left)
    ]


def dataset_right_anti_join_tables(
    left: Dataset,
    right: Iterable[pa.Table],
    *,
    keys: Union[str, List[str]],
    right_keys: Union[str, List[str], None] = None,
    schema_table: pa.Table,
    # start_result_table_index: int = 0,
    result_label: int = 0,
    use_threads: bool = True,
) -> List[TableFilterRefPair]:
    shared_args = _WrappedAntiJoinSharedArgs(
        keys=keys,
        right_keys=right_keys,
        schema_table=schema_table,
        result_label=result_label,
        use_threads=use_threads,
    )
    shared_args_ref = ray.put(shared_args)

    return [
        _wrapped_dataset_right_anti_join_table.remote(
            left=left,
            right=right_table,
            shared_args=shared_args_ref,
            # keys=keys,
            # right_keys=right_keys,
            # schema_table=schema_table,
            result_table_index=i,
            # result_label=result_label,
            # use_threads=use_threads,
        )
        for i, right_table in enumerate(right)
    ]


@ray.remote(num_returns=2)
def _wrapped_table_left_anti_join_dataset(
    left: pa.Table,
    right: Dataset,
    *,
    shared_args: _WrappedAntiJoinSharedArgs,
    # keys: Union[str, List[str]],
    # right_keys: Union[str, List[str], None] = None,
    # schema_table: pa.Table,
    result_table_index: int,
    # result_label: int = 0,
    # use_threads: bool = True,
) -> TableFilterPair:
    anti_join_result = table_left_anti_join_dataset(
        left=left,
        right=right,
        keys=shared_args.keys,
        right_keys=shared_args.right_keys,
        use_threads=shared_args.use_threads,
    )

    formatted_result = pa.concat_tables(
        [shared_args.schema_table, anti_join_result], promote=True
    )
    filter_info = make_empty_table_filter_info(
        obj=formatted_result, index=result_table_index, label=shared_args.result_label
    )

    return formatted_result, filter_info


@ray.remote(num_returns=2)
def _wrapped_dataset_right_anti_join_table(
    left: Dataset,
    right: pa.Table,
    *,
    shared_args: _WrappedAntiJoinSharedArgs,
    # keys: Union[str, List[str]],
    # right_keys: Union[str, List[str], None] = None,
    # schema_table: pa.Table,
    result_table_index: int,
    # result_label: int = 0,
    # use_threads: bool = True,
) -> TableFilterPair:
    anti_join_result = dataset_right_anti_join_table(
        left=left,
        right=right,
        keys=shared_args.keys,
        right_keys=shared_args.right_keys,
        use_threads=shared_args.use_threads,
    )

    formatted_result = pa.concat_tables(
        [shared_args.schema_table, anti_join_result], promote=True
    )
    filter_info = make_empty_table_filter_info(
        obj=formatted_result, index=result_table_index, label=shared_args.result_label
    )

    return formatted_result, filter_info
