from pathlib import Path
from typing import List, Tuple


def get_datasets_tables_from_modified_files(
    modified_files: List[str],
) -> List[Tuple[str, str]]:
    """
    Returns a list of (dataset_id, table_id) from the list of modified files.

    Args:
        modified_files (List[str]): List of modified files.

    Returns:
        List[Tuple[str, str]]: List of (dataset_id, table_id).
    """
    # Convert to Path
    modified_files: List[Path] = [Path(file) for file in modified_files]

    # Get SQL files
    sql_files: List[Path] = [file for file in modified_files if file.suffix == ".sql"]

    # Extract dataset_id and table_id from SQL files
    datasets_tables: List[Tuple[str, str]] = [
        (file.parent.name, file.stem) for file in sql_files
    ]

    # Post-process table_id:
    # - Some of `table_id` will have the format `{dataset_id}__{table_id}`. We must
    #   remove the `{dataset_id}__` part.
    new_datasets_tables: List[Tuple[str, str]] = []
    for dataset_id, table_id in datasets_tables:
        crop_str = f"{dataset_id}__"
        if table_id.startswith(crop_str):
            table_id = table_id[len(crop_str) :]
        new_datasets_tables.append((dataset_id, table_id))
    datasets_tables = new_datasets_tables

    # Get schema files
    schema_files: List[Path] = [
        file
        for file in modified_files
        if file.name.startswith("schema")
        and (file.suffix == ".yaml" or file.suffix == ".yml")
    ]

    # Extract dataset_id and table_id from schema files
    for schema_file in schema_files:
        dataset_id = schema_file.parent.name
        table_id = "__all__"
        datasets_tables.append((dataset_id, table_id))

    return datasets_tables
