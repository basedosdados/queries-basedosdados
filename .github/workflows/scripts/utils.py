from pathlib import Path
from typing import List, Tuple, Union


def get_datasets_tables_from_modified_files(
    modified_files: List[str],
    show_deleted: bool = False,
) -> Union[List[Tuple[str, str]], List[Tuple[str, str, bool]]]:
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
    if not show_deleted:
        datasets_tables: List[Tuple[str, str]] = [
            (file.parent.name, file.stem) for file in sql_files
        ]
    else:
        datasets_tables: List[Tuple[str, str, bool]] = [
            (file.parent.name, file.stem, file.exists()) for file in sql_files
        ]

    # Post-process table_id:
    # - Some of `table_id` will have the format `{dataset_id}__{table_id}`. We must
    #   remove the `{dataset_id}__` part.
    new_datasets_tables: List[Tuple[str, str]] = []
    if not show_deleted:
        for dataset_id, table_id in datasets_tables:
            crop_str = f"{dataset_id}__"
            if table_id.startswith(crop_str):
                table_id = table_id[len(crop_str) :]
            new_datasets_tables.append((dataset_id, table_id))
    else:
        for dataset_id, table_id, exists in datasets_tables:
            crop_str = f"{dataset_id}__"
            if table_id.startswith(crop_str):
                table_id = table_id[len(crop_str) :]
            new_datasets_tables.append((dataset_id, table_id, exists))
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
        if not show_deleted:
            datasets_tables.append((dataset_id, table_id))
        else:
            datasets_tables.append((dataset_id, table_id, schema_file.exists()))

    return datasets_tables
