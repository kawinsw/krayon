from collections import Counter
from collections.abc import Hashable
from typing import Iterable, List, Mapping, TypeVar

import pyarrow as pa
from ray.data import Dataset

from krayon.utils import PYARROW_BATCH_FORMAT

H = TypeVar("H", bound=Hashable)


def rename_dataset_columns(
    dataset: Dataset, name_changes: Mapping[str, str]
) -> Dataset:
    """
    Renames columns in the input `dataset` as specified in `name_changes`.

    Args:
        dataset:
            Input Ray `Dataset` whose columns are to be renamed.
        name_changes:
            For each column to be renamed, maps the old name to the new name.

    Returns:
        Copy of the input `dataset` with columns renamed
        as specified in `name_changes`.
    """
    # Validate that resulting names are all distinct
    aft_names = transform(dataset.columns(), name_changes)
    if len(aft_names) != len(set(aft_names)):
        repeats = ", ".join(
            f"{aft_name} ({count})"
            for aft_name, count in Counter(aft_names).items()
            if count > 1
        )
        raise ValueError(
            f"Renamed columns would have the following repeated names: {repeats}"
        )

    return dataset.map_batches(
        rename_table_columns,
        batch_format=PYARROW_BATCH_FORMAT,
        zero_copy_batch=True,
        fn_kwargs=dict(name_changes=name_changes),
    )


def rename_table_columns(table: pa.Table, name_changes: Mapping[str, str]) -> pa.Table:
    """
    Returns a copy of the input `table` with columns renamed
    as specified in `name_changes`.
    """
    return table.rename_columns(transform(table.column_names, name_changes))


def transform(
    before: Iterable[H], mapping: Mapping[H, H]
) -> List[H]:
    """
    For each element in `before`, returns the corresponding value
    in the transformation `mapping` if found,
    otherwise returns the element unchanged.
    """
    return [mapping.get(bef, bef) for bef in before]
