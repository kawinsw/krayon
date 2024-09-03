from typing import List, Union

import pyarrow as pa
from ray.data import Dataset

from krayon.utils import PYARROW_BATCH_FORMAT
from krayon.join.left_anti_join import table_left_anti_join_dataset


def left_semi_join_datasets(
    left: Dataset,
    right: Dataset,
    keys: Union[str, List[str]],
    right_keys: Union[str, List[str], None] = None,
    use_threads: bool = True,
) -> Dataset:
    """
    Performs a distributed left semi join on `left` and `right`.

    Args:
        left:
        right:
        keys:
        right_keys:
        use_threads:

    Returns:
        `Dataset` containing each row in `left` that has at least one match in `right`.
    """
    fn_kwargs = dict(
        right=right,
        keys=keys,
        right_keys=right_keys,
        use_threads=use_threads,
    )
    return left.map_batches(
        table_left_semi_join_dataset,
        batch_format=PYARROW_BATCH_FORMAT,
        fn_kwargs=fn_kwargs,
    )


def table_left_semi_join_dataset(
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
        `Table` containing each row in `left` that has at least one match in `right`.
    """
    anti_join_result = table_left_anti_join_dataset(
        left=left,
        right=right,
        keys=keys,
        right_keys=right_keys,
        use_threads=use_threads,
    )

    return left.join(
        join_type="left anti",
        right_table=anti_join_result,
        keys=keys,
        use_threads=use_threads,
    )
