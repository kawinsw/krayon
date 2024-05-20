from collections import Counter
from typing import Container, Dict, Iterable, List, Optional, Tuple

from krayon.rename_columns import transform


def resolve_column_names(
    left_names: List[str],
    right_names: List[str],
    key_pairs: List[Tuple[str, str]],
    left_suffix: Optional[str] = None,
    right_suffix: Optional[str] = None,
    coalesce_keys: bool = True,
) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Determines the column rename mappings.
    If `coalesce_keys`, omits columns from the right `Dataset` that are being joined on.
    To omit columns from the left `Dataset` instead, swap sides (left/right)
    for all inputs (including `key_pairs`) and outputs.

    Raises a `ValueError` if the given inputs would result in
    invalid output column names.

    Args:
        left_names:
            Column names from the left `Dataset`.
        right_names:
            Column names from the right `Dataset`.
        key_pairs:
            `(left_key, right_key)` indicating each pair of corresponding columns
            that the `Dataset`s are being joined on.
        left_suffix:
            Suffix to append to each column name from the left `Dataset`
            that is also (initially) a column name in the right `Dataset`.
        right_suffix:
            Suffix to append to each column name from the right `Dataset`
            that is also (initially) a column name in the left `Dataset`.
        coalesce_keys:
            Whether to omit columns from the right `Dataset` that are being joined on.
            To omit columns from the left `Dataset` instead,
            swap sides (left/right) for all inputs and outputs.

    Returns:
        `(left_renames, right_renames)` where each `dict` maps each initial name
        to the corresponding final name for names that are being changed.
    """
    # Append the appropriate suffix to each column name
    # that is also (initially) a column name in the other `Dataset`
    left_renames = _append_suffix_if_in(left_names, set(right_names), left_suffix)
    right_renames = _append_suffix_if_in(right_names, set(left_names), right_suffix)

    if coalesce_keys:
        # Check for repeated right keys
        right_key_counter = Counter(right_key for _, right_key in key_pairs)
        right_key_repeats = [
            f"{right_key} (x{count})"
            for right_key, count in right_key_counter.items()
            if count > 1
        ]
        if right_key_repeats:
            raise ValueError("Repeated right keys: " + ", ".join(right_key_repeats))

        # Override renames for columns that are being joined on
        for key, right_key in key_pairs:
            if key == right_key:
                right_renames.pop(right_key, None)
            else:
                right_renames[right_key] = key  # rename to match left
            left_renames.pop(right_key, None)  # if exists, no longer needs a suffix

        # Right columns that are being joined on will be renamed to match left
        # so exclude these from the check for repeated output names
        right_bef_names = [
            name for name in right_names if name not in right_key_counter
        ]
    else:
        right_bef_names = right_names

    # Check for repeated output names
    right_aft_names = transform(right_bef_names, right_renames)
    left_aft_names = transform(left_names, left_renames)
    aft_names = left_aft_names + right_aft_names

    if len(aft_names) == len(set(aft_names)):
        return left_renames, right_renames
    else:
        aft_repeats = [
            f"{aft_name} (x{count})"
            for aft_name, count in Counter(aft_names).items()
            if count > 1
        ]
        raise ValueError(
            "Inputs result in repeated output column names: " + ", ".join(aft_repeats)
        )


def _append_suffix_if_in(
    base_strs: Iterable[str], include: Container[str], suffix: Optional[str] = None
) -> Dict[str, str]:
    return {s: f"{s}{suffix}" for s in base_strs if s in include} if suffix else {}
