import functools
from typing import List, Union

import pyarrow as pa
from ray.data import Dataset
from ray.data.block import Block
from ray.data.aggregate import AggregateFn

from krayon.join.utils import (
    PYARROW_BATCH_FORMAT,
    block_to_table,
    intersect_tables,
    validate_join_keys,
)


def right_anti_join_datasets(
    left: Dataset,
    right: Dataset,
    keys: Union[str, List[str]],
    right_keys: Union[str, List[str], None] = None,
    use_threads: bool = True,
) -> Dataset:
    """
    Performs a distributed right anti join on `left` and `right`.

    Returns:
        `Dataset` containing each row in `right` that has no match in `left`.
    """
    validate_join_keys(...)  # TODO

    fn_kwargs = dict(
        left=left,
        keys=keys,
        right_keys=right_keys,
        use_threads=use_threads,
    )

    return right.map_batches(
        _dataset_right_anti_join_table,
        batch_format=PYARROW_BATCH_FORMAT,
        fn_kwargs=fn_kwargs,
    )


def dataset_right_anti_join_table(
    left: Dataset,
    right: pa.Table,
    keys: Union[str, List[str]],
    right_keys: Union[str, List[str], None] = None,
    use_threads: bool = True,
) -> pa.Table:
    """
    TODO: docstring
    Does not perform any validation.

    Returns:
        `Table` containing each row in `right` that has no match in `left`.
    """
    def block_right_anti_join_table(left_block: Block, right_accum: pa.Table) -> pa.Table:
        return block_to_table(left_block).join(
            join_type="right anti",
            right_table=right_accum,
            keys=keys,
            right_keys=right_keys,
            use_threads=use_threads,
        )

    agg = AggregateFn(
        init=lambda _: right,
        accumulate_block=block_right_anti_join_table,
        merge=functools.partial(intersect_tables, use_threads=use_threads),
        name="right anti join",  # TODO: Mandatory? Must be unique?
    )

    return left.aggregate(agg)


def _dataset_right_anti_join_table(
    right: pa.Table,  # batch must be the first positional argument
    left: Dataset,
    keys: Union[str, List[str]],
    right_keys: Union[str, List[str], None] = None,
    use_threads: bool = True,
) -> pa.Table:
    """
    TODO: docstring
    Does not perform any validation.

    NOTE:
    Here, the usual intuitive order for `left` and `right` is swapped
    and we cannot force them to be keyword-only arguments
    because Ray's `Dataset.map_batches()` API requires the batch (`right`) 
    to be the first positional argument.
    To minimize confusion, instead use `dataset_right_anti_join_table()`,
    which has the usual intuitive order (`left` then `right`).
    If unavoidable, specify arguments by name (i.e. as keyword arguments)
    instead of just by position.

    Returns:
        `Table` containing each row in `right` that has no match in `left`.
    """
    return dataset_right_anti_join_table(
        left=left,
        right=right,
        keys=keys,
        right_keys=right_keys,
        use_threads=use_threads,
    )
