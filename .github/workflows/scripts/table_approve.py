from argparse import ArgumentParser

from backend import Backend
from utils import expand_alls, get_datasets_tables_from_modified_files

if __name__ == "__main__":
    # Start argument parser
    arg_parser = ArgumentParser()

    # Add GraphQL URL argument
    arg_parser.add_argument(
        "--graphql-url",
        type=str,
        required=True,
        help="URL of the GraphQL endpoint.",
    )

    # Add list of modified files argument
    arg_parser.add_argument(
        "--modified-files",
        type=str,
        required=True,
        help="List of modified files.",
    )

    # Get arguments
    args = arg_parser.parse_args()

    # Get datasets and tables from modified files
    modified_files = args.modified_files.split(",")
    datasets_tables = get_datasets_tables_from_modified_files(
        modified_files, show_deleted=True
    )

    # Split deleted datasets and tables
    deleted_datasets_tables = []
    existing_datasets_tables = []
    for dataset_id, table_id, exists in datasets_tables:
        if exists:
            existing_datasets_tables.append((dataset_id, table_id))
        else:
            deleted_datasets_tables.append((dataset_id, table_id))

    # Initialize backend
    backend = Backend(args.graphql_url)

    # Expand `__all__` tables
    expanded_existing_datasets_tables = []
    for dataset_id, table_id in existing_datasets_tables:
        expanded_existing_datasets_tables.extend(
            expand_alls(dataset_id, table_id, backend)
        )
    existing_datasets_tables = expanded_existing_datasets_tables
