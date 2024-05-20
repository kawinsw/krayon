from typing import Generator, Iterable, List, Optional, Union

import pyarrow as pa
import ray
from ray.data import Dataset

from krayon.join._column_name_utils import resolve_column_names
from krayon.join._filter_utils import filter_refs_in_groups, wait_and_filter
from krayon.join._inner_join_utils import InnerJoinArgs, inner_join_tables
from krayon.join._table_filter_utils import (
    TableFilterRefPair,
    table_is_not_empty_ref,
)
from krayon.join.utils import (
    ObjectOrRef,
    TableOrRef,
    ensure_object_ref,
    make_key_pairs,
    validate_join_keys,
)
from krayon.rename_columns import rename_dataset_columns
from krayon.utils import PYARROW_BATCH_FORMAT


def inner_join_datasets(
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
    Performs a distributed inner join on `left` and `right`.

    NOTE:
    Blocks and materializes (in Ray object store) all data
    in both `left` and `right` input `Dataset`s,
    as well as all data in the resulting joined `Dataset`.

    Args:
        left:
        right:
        keys:
        right_keys:
        left_suffix:
        right_suffix:
        coalesce_keys:
        use_threads:

    Returns:
        Result of inner join
    """
    validate_join_keys(join_type="inner", keys=keys, right_keys=right_keys)

    result_gen = tables_inner_join_tables(
        left_tables=left.to_arrow_refs(),
        right_tables=right.to_arrow_refs(),
        keys=keys,
        right_keys=right_keys,
        left_suffix=left_suffix,
        right_suffix=right_suffix,
        coalesce_keys=coalesce_keys,
        use_threads=use_threads,
    )
    result_ref_pairs = list(result_gen)

    # Blocks and materializes results of join (in Ray object store)
    filter_info_refs = [filter_info_ref for _, filter_info_ref in result_ref_pairs]
    table_refs = [
        result_ref_pairs[filter_info.index][0]
        for filter_info in wait_and_filter(filter_info_refs)
    ]

    if len(table_refs) > 0:
        return ray.data.from_arrow_refs(table_refs)
    else:  # Construct Dataset with appropriate schema
        # Currently, this actually makes no difference;
        # Ray ignores empty blocks, so an empty Dataset effectively has no schema

        # Manually determine output column names
        key_pairs = make_key_pairs(keys, right_keys)
        left_renames, right_renames = resolve_column_names(
            left_names=left.columns(),
            right_names=right.columns(),
            key_pairs=key_pairs,
            left_suffix=left_suffix,
            right_suffix=right_suffix,
            coalesce_keys=coalesce_keys,
        )
        renamed_left = rename_dataset_columns(left.limit(1), left_renames)
        renamed_right = rename_dataset_columns(right.limit(1), right_renames)

        # Obtain output schema
        left_schema = renamed_left.take_batch(
            batch_size=1, batch_format=PYARROW_BATCH_FORMAT
        ).schema
        right_sample = renamed_right.take_batch(
            batch_size=1, batch_format=PYARROW_BATCH_FORMAT
        )
        if coalesce_keys:
            right_sample.drop(columns={right_key for _, right_key in key_pairs})
        output_schema = pa.unify_schemas([left_schema, right_sample.schema])

        return ray.data.from_arrow(output_schema.empty_table())


def tables_inner_join_tables(
    left_tables: Iterable[TableOrRef],
    right_tables: Iterable[TableOrRef],
    keys: Union[str, List[str]],
    right_keys: Union[str, List[str], None] = None,
    left_suffix: Optional[str] = None,
    right_suffix: Optional[str] = None,
    coalesce_keys: bool = True,
    use_threads: bool = True,
    schema_table: ObjectOrRef[Optional[pa.Table]] = None,
    result_label: ObjectOrRef[int] = 0,
) -> Generator[TableFilterRefPair, None, None]:
    """
    Performs a distributed inner join over all entries in `left_tables`
    with all entries in `right_tables`.
    Does not perform any validation.

    NOTE: Materializes all results of the join (in Ray object store).

    Args:
        left:
        right:
        keys:
        right_keys:
        left_suffix:
            Suffix to append to each column name from the left `Dataset`
            that is also (initially) a column name in the right `Dataset`.
        right_suffix:
            Suffix to append to each column name from the right `Dataset`
            that is also (initially) a column name in the left `Dataset`.
        coalesce_keys:
            Whether to omit columns from the right `Dataset` that are being joined on.
        schema_table:
            TODO
        use_threads:
            Whether to use multithreading.

    Yields:
        Results of inner join as `(table, filter_info)` pairs,
        where `table` and `filter_info` are each wrapped in an `ObjectRef`.
        May contain empty `Table`s.
    """
    # Make ObjectRef for inner join args
    inner_join_args: InnerJoinArgs = dict(
        keys=keys,
        right_keys=right_keys,
        left_suffix=left_suffix,
        right_suffix=right_suffix,
        coalesce_keys=coalesce_keys,
        use_threads=use_threads,
    )
    inner_join_args_ref = ray.put(inner_join_args)
    schema_table_ref = ensure_object_ref(schema_table)
    result_label_ref = ensure_object_ref(result_label)

    # Filter out any empty blocks from both `left` and `right`
    left_table_list = list(left_tables)
    right_table_list = list(right_tables)
    # left_table_info_refs = make_all_filter_info(
    #     targets=left_table_list, start_index=0, filter_fn=is_not_empty_ref
    # )
    # num_left_tables = len(left_table_list)
    # right_table_info_refs = make_all_filter_info(
    #     targets=right_table_list, start_index=num_left_tables, filter_fn=is_not_empty_ref
    # )
    # all_tables = left_table_list + right_table_list

    # Join each pair of blocks in the Cartesian product of `left` and `right` blocks

    filtered_left: List[TableOrRef] = []
    filtered_right: List[TableOrRef] = []
    result_table_index = 0

    for table_ref, group_index in filter_refs_in_groups(
        [left_table_list, right_table_list], table_is_not_empty_ref
    ):
        # table_ref = all_tables[table_index]
        # num_refs = len(result_ref_tuples)

        if group_index == 0:  # from `left`
            filtered_left.append(table_ref)
            for right_table_ref in filtered_right:
                yield inner_join_tables.remote(
                    left=table_ref,
                    right=right_table_ref,
                    inner_join_args=inner_join_args_ref,
                    schema_table=schema_table_ref,
                    result_table_index=result_table_index,
                    result_label=result_label_ref,
                )
                result_table_index += 1
        elif group_index == 1:  # from `right`
            filtered_right.append(table_ref)
            for left_table_ref in filtered_left:
                yield inner_join_tables.remote(
                    left=left_table_ref,
                    right=table_ref,
                    inner_join_args=inner_join_args_ref,
                    schema_table=schema_table_ref,
                    result_table_index=result_table_index,
                    result_label=result_label_ref,
                )
                result_table_index += 1
        else:  # this should never happen
            raise RuntimeError(f"Invalid group_index: {group_index}")
