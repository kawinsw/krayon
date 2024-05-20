from typing import List, Optional, Union

from ray.data import Dataset

from krayon.join.inner_join import inner_join_datasets
from krayon.join.left_semi_join import left_semi_join_datasets
from krayon.join.right_semi_join import right_semi_join_datasets
from krayon.join.left_anti_join import left_anti_join_datasets
from krayon.join.right_anti_join import right_anti_join_datasets
from krayon.join.outer_join import (
    full_outer_join_datasets,
    left_outer_join_datasets,
    right_outer_join_datasets,
)
from krayon.join.utils import JoinType


def join_datasets(
    left: Dataset,
    right: Dataset,
    join_type: JoinType,
    keys: Union[str, List[str]],
    right_keys: Union[str, List[str], None] = None,
    left_suffix: Optional[str] = None,
    right_suffix: Optional[str] = None,
    coalesce_keys: bool = True,
    use_threads: bool = True,
) -> Dataset:
    """
    TODO: docstring
    """
    if join_type == "inner":
        return inner_join_datasets(
            left=left,
            right=right,
            keys=keys,
            right_keys=right_keys,
            left_suffix=left_suffix,
            right_suffix=right_suffix,
            coalesce_keys=coalesce_keys,
            use_threads=use_threads,
        )
    elif join_type == "left semi":
        return left_semi_join_datasets(
            left=left,
            right=right,
            keys=keys,
            right_keys=right_keys,
            use_threads=use_threads,
        )
    elif join_type == "right semi":
        return right_semi_join_datasets(
            left=left,
            right=right,
            keys=keys,
            right_keys=right_keys,
            use_threads=use_threads,
        )
    elif join_type == "left anti":
        return left_anti_join_datasets(
            left=left,
            right=right,
            keys=keys,
            right_keys=right_keys,
            use_threads=use_threads,
        )
    elif join_type == "right anti":
        return right_anti_join_datasets(
            left=left,
            right=right,
            keys=keys,
            right_keys=right_keys,
            use_threads=use_threads,
        )
    elif join_type == "full outer":
        return full_outer_join_datasets(
            left=left,
            right=right,
            keys=keys,
            right_keys=right_keys,
            left_suffix=left_suffix,
            right_suffix=right_suffix,
            coalesce_keys=coalesce_keys,
            use_threads=use_threads,
        )
    elif join_type == "left outer":
        return left_outer_join_datasets(
            left=left,
            right=right,
            keys=keys,
            right_keys=right_keys,
            left_suffix=left_suffix,
            right_suffix=right_suffix,
            coalesce_keys=coalesce_keys,
            use_threads=use_threads,
        )
    elif join_type == "right outer":
        return right_outer_join_datasets(
            left=left,
            right=right,
            keys=keys,
            right_keys=right_keys,
            left_suffix=left_suffix,
            right_suffix=right_suffix,
            coalesce_keys=coalesce_keys,
            use_threads=use_threads,
        )
    else:
        raise TypeError(f"Invalid join_type: {join_type}")
