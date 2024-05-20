"""Distributed outer joins on Ray `Dataset`s."""
from typing import List, Optional, Union

import pyarrow as pa
import ray
from ray.data import Dataset

from krayon.join._filter_utils import wait_and_filter
from krayon.join.inner_join import tables_inner_join_tables
from krayon.join._column_name_utils import resolve_column_names
from krayon.join.outer_join._wrapped_anti_join import (
    dataset_right_anti_join_tables,
    tables_left_anti_join_dataset,
)
from krayon.join.utils import make_key_pairs
from krayon.rename_columns import transform, rename_dataset_columns
from krayon.utils import PYARROW_BATCH_FORMAT


def full_outer_join_datasets(
    left: Dataset,
    right: Dataset,
    keys: Union[str, List[str]],
    right_keys: Union[str, List[str], None] = None,
    left_suffix: Optional[str] = None,
    right_suffix: Optional[str] = None,
    coalesce_keys: bool = True,
    use_threads: bool = True,
) -> Dataset:
    """
    Performs a distributed full outer join on `left` and `right`.

    NOTE: Behavior not guaranteed if `left` and/or `right` is/are empty.
    """
    # Validate and standardize format for join keys
    key_pairs = list(make_key_pairs(keys, right_keys))

    # Determine output column names
    left_renames, right_renames = resolve_column_names(
        left_names=left.columns(),
        right_names=right.columns(),
        key_pairs=key_pairs,
        left_suffix=left_suffix,
        right_suffix=right_suffix,
        coalesce_keys=coalesce_keys,
    )

    # Set output column names
    renamed_left = rename_dataset_columns(left, left_renames)
    renamed_right = rename_dataset_columns(right, right_renames)

    left_key_list = [left_key for left_key, _ in key_pairs]
    renamed_left_keys = transform(left_key_list, left_renames)
    right_key_list = [right_key for _, right_key in key_pairs]
    renamed_right_keys = transform(right_key_list, right_renames)

    # Obtain empty Table with schema
    left_schema = renamed_left.take_batch(
        batch_size=1, batch_format=PYARROW_BATCH_FORMAT
    ).schema
    right_sample = renamed_right.take_batch(
        batch_size=1, batch_format=PYARROW_BATCH_FORMAT
    )
    if coalesce_keys:
        right_sample = right_sample.drop(columns=set(renamed_right_keys))
    right_schema = right_sample.schema
    schema_table = pa.unify_schemas([left_schema, right_schema]).empty_table()

    left_tables = renamed_left.to_arrow_refs()
    right_tables = renamed_right.to_arrow_refs()
    # inner_join_results_len = len(left_tables) * len(right_tables)

    # Inner join
    inner_join_gen = tables_inner_join_tables(
        left_tables=left_tables,
        right_tables=right_tables,
        keys=renamed_left_keys,
        right_keys=renamed_right_keys,
        coalesce_keys=coalesce_keys,
        use_threads=use_threads,
        schema_table=schema_table,
    )
    inner_join_list = list(inner_join_gen)

    # Left anti join
    left_anti_join_list = tables_left_anti_join_dataset(
        left=left_tables,
        right=renamed_right,
        keys=renamed_left_keys,
        right_keys=renamed_right_keys,
        use_threads=use_threads,
        schema_table=schema_table,
        result_label=1,
    )
    #     _wrapped_table_left_anti_join_dataset.remote(
    #         left=left_table,
    #         right=renamed_right,
    #         keys=renamed_left_keys,
    #         right_keys=renamed_right_keys,
    #         schema_table=schema_table_ref,
    #         result_table_index=i,
    #         use_threads=use_threads,
    #     )
    #     for i, left_table in enumerate(left_tables, start=inner_join_results_len)
    # ]

    # Right anti join
    # right_index_start = inner_join_results_len + len(left_anti_join_results)
    right_anti_join_list = dataset_right_anti_join_tables(
        left=renamed_left,
        right=right_tables,
        keys=renamed_left_keys,
        right_keys=renamed_right_keys,
        use_threads=use_threads,
        schema_table=schema_table,
        result_label=2,
    )
    # [
    #     _wrapped_dataset_right_anti_join_table.remote(
    #         left=renamed_left,
    #         right=right_table,
    #         keys=renamed_left_keys,
    #         right_keys=renamed_right_keys,
    #         schema_table=schema_table_ref,
    #         result_table_index=i,
    #         use_threads=use_threads,
    #     )
    #     for i, right_table in enumerate(right_tables, start=right_index_start)
    # ]

    # Create output Dataset from non-empty table_refs
    # NOTE: Blocks and materializes results of join (in Ray object store)

    result_lists = [inner_join_list, left_anti_join_list, right_anti_join_list]
    table_refs = [
        result_lists[filter_info.label][filter_info.index][0]
        for filter_info in wait_and_filter(
            filter_info_ref
            for result_list in result_lists
            for _, filter_info_ref in result_list
        )
    ]

    return (
        ray.data.from_arrow_refs(table_refs)
        if len(table_refs > 0)
        else ray.data.from_arrow(schema_table)
    )


def left_outer_join_datasets(
    left: Dataset,
    right: Dataset,
    keys: Union[str, List[str]],
    right_keys: Union[str, List[str], None] = None,
    left_suffix: Optional[str] = None,
    right_suffix: Optional[str] = None,
    coalesce_keys: bool = True,
    use_threads: bool = True,
) -> Dataset:
    """
    Performs a distributed left outer join on `left` and `right`.

    NOTE: Behavior not guaranteed if `left` and/or `right` is/are empty.
    """
    # Validate and standardize format for join keys
    key_pairs = list(make_key_pairs(keys, right_keys))

    # Determine output column names
    left_renames, right_renames = resolve_column_names(
        left_names=left.columns(),
        right_names=right.columns(),
        key_pairs=key_pairs,
        left_suffix=left_suffix,
        right_suffix=right_suffix,
        coalesce_keys=coalesce_keys,
    )

    # Set output column names
    renamed_left = rename_dataset_columns(left, left_renames)
    renamed_right = rename_dataset_columns(right, right_renames)

    left_key_list = [left_key for left_key, _ in key_pairs]
    renamed_left_keys = transform(left_key_list, left_renames)
    right_key_list = [right_key for _, right_key in key_pairs]
    renamed_right_keys = transform(right_key_list, right_renames)

    # Obtain empty Table with schema
    left_schema = renamed_left.take_batch(
        batch_size=1, batch_format=PYARROW_BATCH_FORMAT
    ).schema
    right_sample = renamed_right.take_batch(
        batch_size=1, batch_format=PYARROW_BATCH_FORMAT
    )
    if coalesce_keys:
        right_sample = right_sample.drop(columns=set(renamed_right_keys))
    right_schema = right_sample.schema
    schema_table = pa.unify_schemas([left_schema, right_schema]).empty_table()

    left_tables = renamed_left.to_arrow_refs()
    right_tables = renamed_right.to_arrow_refs()

    # Inner join
    inner_join_gen = tables_inner_join_tables(
        left_tables=left_tables,
        right_tables=right_tables,
        keys=renamed_left_keys,
        right_keys=renamed_right_keys,
        coalesce_keys=coalesce_keys,
        use_threads=use_threads,
        schema_table=schema_table,
        result_label=0,
    )
    inner_join_list = list(inner_join_gen)

    # Left anti join
    left_anti_join_list = tables_left_anti_join_dataset(
        left=left_tables,
        right=renamed_right,
        keys=renamed_left_keys,
        right_keys=renamed_right_keys,
        use_threads=use_threads,
        schema_table=schema_table,
        result_label=1,
    )

    # Create output Dataset from non-empty table_refs
    # NOTE: Blocks and materializes results of join (in Ray object store)

    result_lists = [inner_join_list, left_anti_join_list]
    table_refs = [
        result_lists[filter_info.label][filter_info.index][0]
        for filter_info in wait_and_filter(
            filter_info_ref
            for result_list in result_lists
            for _, filter_info_ref in result_list
        )
    ]

    return (
        ray.data.from_arrow_refs(table_refs)
        if len(table_refs > 0)
        else ray.data.from_arrow(schema_table)
    )


def right_outer_join_datasets(
    left: Dataset,
    right: Dataset,
    keys: Union[str, List[str]],
    right_keys: Union[str, List[str], None] = None,
    left_suffix: Optional[str] = None,
    right_suffix: Optional[str] = None,
    coalesce_keys: bool = True,
    use_threads: bool = True,
) -> Dataset:
    """
    Performs a distributed right outer join on `left` and `right`.

    NOTE: Behavior not guaranteed if `left` and/or `right` is/are empty.
    """
    # Validate and standardize format for join keys
    key_pairs = list(make_key_pairs(keys, right_keys))

    # Determine output column names
    # Swap sides to omit left (instead of right) column names if `coalesce_keys`
    right_renames, left_renames = resolve_column_names(
        left_names=right.columns(),
        right_names=left.columns(),
        key_pairs=[(right_key, left_key) for left_key, right_key in key_pairs],
        left_suffix=right_suffix,
        right_suffix=left_suffix,
        coalesce_keys=coalesce_keys,
    )

    # Set output column names
    renamed_left = rename_dataset_columns(left, left_renames)
    renamed_right = rename_dataset_columns(right, right_renames)

    left_key_list = [left_key for left_key, _ in key_pairs]
    renamed_left_keys = transform(left_key_list, left_renames)
    right_key_list = [right_key for _, right_key in key_pairs]
    renamed_right_keys = transform(right_key_list, right_renames)

    # Obtain empty Table with schema
    left_sample = renamed_left.take_batch(
        batch_size=1, batch_format=PYARROW_BATCH_FORMAT
    )
    if coalesce_keys:
        left_sample = left_sample.drop(columns=set(renamed_left_keys))
    left_schema = left_sample.schema
    right_schema = renamed_right.take_batch(
        batch_size=1, batch_format=PYARROW_BATCH_FORMAT
    ).schema
    schema_table = pa.unify_schemas([left_schema, right_schema]).empty_table()

    left_tables = renamed_left.to_arrow_refs()
    right_tables = renamed_right.to_arrow_refs()

    # Inner join
    inner_join_gen = tables_inner_join_tables(
        left_tables=left_tables,
        right_tables=right_tables,
        keys=renamed_left_keys,
        right_keys=renamed_right_keys,
        coalesce_keys=coalesce_keys,
        use_threads=use_threads,
        schema_table=schema_table,
        result_label=0,
    )
    inner_join_list = list(inner_join_gen)

    # Right anti join
    right_anti_join_list = dataset_right_anti_join_tables(
        left=renamed_left,
        right=right_tables,
        keys=renamed_left_keys,
        right_keys=renamed_right_keys,
        use_threads=use_threads,
        schema_table=schema_table,
        result_label=1,
    )

    # Create output Dataset from non-empty table_refs
    # NOTE: Blocks and materializes results of join (in Ray object store)

    result_lists = [inner_join_list, right_anti_join_list]
    table_refs = [
        result_lists[filter_info.label][filter_info.index][0]
        for filter_info in wait_and_filter(
            filter_info_ref
            for result_list in result_lists
            for _, filter_info_ref in result_list
        )
    ]

    return (
        ray.data.from_arrow_refs(table_refs)
        if len(table_refs > 0)
        else ray.data.from_arrow(schema_table)
    )
