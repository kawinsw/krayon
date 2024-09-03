import itertools
from random import Random
from typing import Dict, Collection, List, Tuple, TypeVar

import pyarrow as pa
import pytest
import ray
from ray.data import Dataset

from krayon import rename_dataset_columns

from ._helpers import ds_to_record_counter, table_to_record_counter

T = TypeVar("T")


_SEED = 25


def _arbitrary_permutation(elems: Collection[T]) -> List[T]:
    return Random(_SEED).sample(elems, len(elems))


@pytest.fixture(scope="module")
def initial_column_names() -> Tuple[str, ...]:
    # 5 columns
    return (
        "20-tEXt_",
        "'!@#$%^&*()",
        "3",
        "",
        "ALL CAPS WITH SPACE",
    )


@pytest.fixture(scope="module")
def column_data() -> Tuple[list, ...]:
    # 5 columns, 20 rows each
    return (
        [f"text-{i}" for i in range(20)],
        list(range(100, 120)),
        [i / 3 for i in range(200, 220)],
        ["lengthwise"[:i] for i in range(20)],
        [7, None, 21, 28] * 5,
    )


@pytest.fixture(scope="module", params=range(1, 5))
def ds_in(
    request: pytest.FixtureRequest,
    initial_column_names: Tuple[str, ...],
    column_data: Tuple[list, ...],
) -> Dict[str, List]:
    data = dict(zip(initial_column_names, column_data))
    table = pa.Table.from_pydict(data)
    return ray.data.from_arrow(table).repartition(request.param)


@pytest.mark.parametrize("changed_column_index", list(range(5)))  # 5 columns
def test_single_change(
    initial_column_names: Tuple[str, ...],
    column_data: Tuple[list, ...],
    ds_in: Dataset,
    changed_column_index: int,
) -> None:
    """Single valid column name change."""
    new_column_name = "08.12!"
    name_changes = {initial_column_names[changed_column_index]: new_column_name}
    ds_out = rename_dataset_columns(ds_in, name_changes)

    expected_column_names = list(initial_column_names)
    expected_column_names[changed_column_index] = new_column_name
    expected_data_dict = dict(zip(expected_column_names, column_data))
    expected_table = pa.Table.from_pydict(expected_data_dict)

    # This should work for any arbitrary permutation of expected_column_names.
    column_names_to_assert = _arbitrary_permutation(expected_column_names)
    actual_record_counter = ds_to_record_counter(ds_out, column_names_to_assert)
    expected_record_counter = table_to_record_counter(
        expected_table, column_names_to_assert
    )
    assert expected_record_counter == actual_record_counter


@pytest.mark.parametrize("changed_column_indices", [(0, 2), (3, 2), (3, 4, 1)])
def test_repeated_new_name_fails(
    initial_column_names: Tuple[str, ...],
    ds_in: Dataset,
    changed_column_indices: Tuple[int, ...],
) -> None:
    new_col_name = "new_col_name"
    name_changes = {
        initial_column_names[i]: new_col_name for i in changed_column_indices
    }
    with pytest.raises(ValueError):
        rename_dataset_columns(ds_in, name_changes)


@pytest.mark.parametrize(
    "changed_column_index, repeated_column_index",
    list(itertools.combinations(range(5), r=2)),
)
def test_repeated_existing_name_fails(
    initial_column_names: Tuple[str, ...],
    ds_in: Dataset,
    changed_column_index: int,
    repeated_column_index: int,
) -> None:
    name_changes = {
        initial_column_names[changed_column_index]:
        initial_column_names[repeated_column_index]
    }
    with pytest.raises(ValueError):
        rename_dataset_columns(ds_in, name_changes)


@pytest.mark.parametrize(
    "cycled_column_indices",
    [(2,), (0, 3), (2, 3), (4, 1), (1, 4, 0), (0, 3, 1, 4, 2), (4, 3, 2, 1, 0)],
)
def test_cycle(
    initial_column_names: Tuple[str, ...],
    column_data: Tuple[list, ...],
    ds_in: Dataset,
    cycled_column_indices: Tuple[int, ...],
) -> None:
    name_changes = {
        initial_column_names[i]: initial_column_names[j]
        for i, j in itertools.pairwise(cycled_column_indices)
    }
    name_changes[initial_column_names[cycled_column_indices[-1]]] = (
        initial_column_names[cycled_column_indices[0]]
    )
    ds_out = rename_dataset_columns(ds_in, name_changes)

    expected_column_names = list(initial_column_names)
    for bef, aft in itertools.pairwise(cycled_column_indices):
        expected_column_names[bef] = initial_column_names[aft]
    expected_column_names[cycled_column_indices[-1]] = (
        initial_column_names[cycled_column_indices[0]]
    )
    expected_data_dict = dict(zip(expected_column_names, column_data))
    expected_table = pa.Table.from_pydict(expected_data_dict)

    # This should work for any arbitrary permutation of expected_column_names.
    column_names_to_assert = _arbitrary_permutation(initial_column_names)
    actual_record_counter = ds_to_record_counter(ds_out, column_names_to_assert)
    expected_record_counter = table_to_record_counter(
        expected_table, column_names_to_assert
    )
    assert expected_record_counter == actual_record_counter


# @pytest.mark.parametrize(
#     "name_changes",
#     [],
# )
# def test_combination(
#     initial_column_names: Tuple[str, ...],
#     column_data: Tuple[list, ...],
#     ds_in: Dataset,
#     name_changes: Dict[str, str],
# ) -> None:
#     ds_out = rename_dataset_columns(ds_in, name_changes)


# @pytest.mark.parametrize(
#     "name_change_indices",
#     [
#         {},  # multiple new names
#         {},  # 1 new name and 3-cycle
#         {},  # 2 new names and self-loop (1-cycle)
#         {},  # 2 new names and swap (2-cycle)
#         {},  # 2 new names and 3-cycle
#         {0: 1, 1: 0, 2: 4, 4: 2},  # 2 swaps (two 2-cycles)
#         {0: 2, 2: 0, 3: 1, 4: 3, 1: 4},  #  swap (2-cycle) and 3-cycle
#     ],
# )
# def test_multiple_cycles(
#     initial_column_names: Tuple[str, ...],
#     column_data: Tuple[list, ...],
#     ds_in: Dataset,
#     name_change_indices: Dict[int, int],
#     table_to_record_counter,
#     ds_to_record_counter,
# ) -> None:
#     name_changes = {
#         initial_column_names[k]: initial_column_names[v]
#         for k, v in name_change_indices.items()
#     }
#     ds_out = rename_dataset_columns(ds_in, name_changes)
    
#     # This should work for any arbitrary permutation of expected_column_names.
#     column_names_to_assert = expected_column_names[[3, 0, 4, 1, 2]]
#     actual_record_counter = ds_to_record_counter(ds_out, column_names_to_assert)
#     expected_record_counter = table_to_record_counter(
#         expected_table, column_names_to_assert
#     )
#     assert expected_record_counter == actual_record_counter
