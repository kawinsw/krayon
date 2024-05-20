from typing import List, Union

import pyarrow as pa
from ray.data import Dataset

from krayon.join.right_anti_join import dataset_right_anti_join_table
from krayon.join.utils import validate_join_keys
from krayon.utils import PYARROW_BATCH_FORMAT


def right_semi_join_datasets(
    left: Dataset,
    right: Dataset,
    keys: Union[str, List[str]],
    right_keys: Union[str, List[str], None] = None,
    use_threads: bool = True,
) -> Dataset:
    """
    Performs a distributed right semi join on `left` and `right`.

    Args:
        left:
        right:
        keys:
        right_keys:
        use_threads:

    Returns:
        `Dataset` containing each row in `right` that has at least one match in `left`.
    """
    validate_join_keys(...)  # TODO
    
    fn_kwargs = dict(
        left=left,
        keys=keys,
        right_keys=right_keys,
        use_threads=use_threads,
    )
    return right.map_batches(
        _dataset_right_semi_join_table,
        batch_format=PYARROW_BATCH_FORMAT,
        fn_kwargs=fn_kwargs,
    )

    # swapped_keys, swapped_right_keys = swap_side_keys(keys, right_keys)

    # return left_semi_join_datasets(
    #     left=right,
    #     right=left,
    #     keys=swapped_keys,
    #     right_keys=swapped_right_keys,
    #     use_threads=use_threads,
    # )


def dataset_right_semi_join_table(
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
        `Table` containing each row in `right` that has at least one match in `left`.
    """
    anti_join_result = dataset_right_anti_join_table(
        left=left,
        right=right,
        keys=keys,
        right_keys=right_keys,
        use_threads=use_threads,
    )

    return right.join(
        join_type="left anti",
        right_table=anti_join_result,
        keys=keys,
        use_threads=use_threads,
    )

    # swapped_keys, swapped_right_keys = swap_side_keys(keys, right_keys)

    # return table_left_semi_join_dataset(
    #     left=right,
    #     right=left,
    #     keys=swapped_keys,
    #     right_keys=swapped_right_keys,
    #     use_threads=use_threads,
    # )


def _dataset_right_semi_join_table(
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
    To minimize confusion, instead use `dataset_right_semi_join_table()`,
    which has the usual intuitive order (`left` then `right`).
    If unavoidable, specify arguments by name (i.e. as keyword arguments)
    instead of just by position.

    Returns:
        `Table` containing each row in `right` that has at least one match in `left`.
    """
    return dataset_right_semi_join_table(
        left=left,
        right=right,
        keys=keys,
        right_keys=right_keys,
        use_threads=use_threads,
    )
