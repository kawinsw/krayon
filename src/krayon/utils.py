from typing import AbstractSet, Iterable, Mapping, Tuple, TypeVar

import pandas as pd
import pyarrow as pa
from ray.data.block import Block

T = TypeVar["T"]

PYARROW_BATCH_FORMAT = "pyarrow"


def block_to_table(block: Block) -> pa.Table:
    if isinstance(block, pa.Table):
        return block
    elif isinstance(block, pd.DataFrame):
        return pa.Table.from_pandas(block)
    else:
        raise TypeError(
            "block must be either a PyArrow Table or Pandas DataFrame, "
            f"but instead is an instance of: {type(block)}"
        )


def get_values_in_tuple(
    mapping: Mapping[str, T], keys: Iterable[str]
) -> Tuple[T, ...]:
    """
    Returns:
        `tuple` containing each value in `mapping` corresponding to
        each element in `keys`, in the order they appear in `keys`.
    Raises:
        KeyError iff any key in `keys` is not in `mapping`.
    """
    return tuple(mapping[key] for key in keys)


def make_unique_str(disallowed: AbstractSet[str]) -> str:
    # TODO: implement
    ...
