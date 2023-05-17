from argparse import ArgumentParser
from typing import List, Tuple

from backend import Backend
from utils import get_datasets_tables_from_modified_files


def expand_alls(
    dataset_id: str, table_id: str, backend: Backend
) -> List[Tuple[str, str]]:
    """
    Expands `__all__` tables into all tables in the dataset.

    Args:
        dataset_id (str): Dataset ID.
        table_id (str): Table ID.
        backend (Backend): Backend class for interacting with the backend.

    Returns:
        List[Tuple[str, str]]: List of tuples with dataset IDs and table IDs.
    """
    if table_id == "__all__":
        table_ids = []
        tables = backend._get_tables_for_dataset(dataset_id)["tables"]
        for table in tables:
            for cloudtable in table["cloudTables"]:
                table_ids.append(cloudtable["gcpTableId"])
    else:
        table_ids = [table_id]
    return [(dataset_id, table_id) for table_id in table_ids]


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

    # Add status argument
    arg_parser.add_argument(
        "--status",
        type=str,
        required=False,
        default="published",
        help="Status to change to.",
    )

    # Add email argument
    arg_parser.add_argument(
        "--email",
        type=str,
        required=True,
        help="Email for authentication.",
    )

    # Add password argument
    arg_parser.add_argument(
        "--password",
        type=str,
        required=True,
        help="Password for authentication.",
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

    # Change status for existing datasets and tables
    print("Authenticating...", end="")
    token = backend._get_token(args.email, args.password)
    print(" OK!")
    for dataset_id, table_id in existing_datasets_tables:
        print(
            f'Changing status of `{dataset_id}.{table_id}` to "{args.status}"...',
            end="",
        )
        backend.modify_status_for_table(
            gcp_dataset_id=dataset_id,
            gcp_table_id=table_id,
            status_slug=args.status,
            token=token,
        )
        print(" Done!")
