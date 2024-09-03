import functools
from typing import List, Union

import pyarrow as pa
from ray.data import Dataset
from ray.data.block import Block
from ray.data.aggregate import AggregateFn

from krayon.join.utils import intersect_tables, validate_join_keys
from krayon.utils import PYARROW_BATCH_FORMAT, block_to_table


def left_anti_join_datasets(
    left: Dataset,
    right: Dataset,
    keys: Union[str, List[str]],
    right_keys: Union[str, List[str], None] = None,
    use_threads: bool = True,
) -> Dataset:
    """
    Performs a distributed left anti join on `left` and `right`.

    Returns:
        `Dataset` containing each row in `left` that has no match in `right`.
    """
    validate_join_keys(...)  # TODO

    fn_kwargs = dict(
        right=right,
        keys=keys,
        right_keys=right_keys,
        use_threads=use_threads,
    )

    return left.map_batches(
        table_left_anti_join_dataset,
        batch_format=PYARROW_BATCH_FORMAT,
        fn_kwargs=fn_kwargs,
    )


def table_left_anti_join_dataset(
    left: pa.Table,
    right: Dataset,
    keys: Union[str, List[str]],
    right_keys: Union[str, List[str], None] = None,
    use_threads: bool = True,
) -> pa.Table:
    """
    TODO: docstring
    Does not perform any validation.

    Returns:
        `Table` containing each row in `left` that has no match in `right`.
    """
    def table_left_anti_join_block(left_accum: pa.Table, right_block: Block) -> pa.Table:
        return left_accum.join(
            join_type="left anti",
            right_table=block_to_table(right_block),
            keys=keys,
            right_keys=right_keys,
            use_threads=use_threads,
        )

    agg = AggregateFn(
        init=lambda _: left,
        accumulate_block=table_left_anti_join_block,
        merge=functools.partial(intersect_tables, use_threads=use_threads),
        name="left anti join",  # TODO: Mandatory? Must be unique?
    )

    return right.aggregate(agg)
