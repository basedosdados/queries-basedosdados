from argparse import ArgumentParser
from copy import deepcopy
import json
from typing import Any, Dict, List, Tuple

from backend import Backend
from utils import get_datasets_tables_from_modified_files


def build_metadata(dataset_id: str, table_id: str, backend: Backend) -> Dict[str, Any]:
    """
    Builds a JSON with the whole metadata for a table.

    Args:
        dataset_id (str): Dataset ID.
        table_id (str): Table ID.
        backend (Backend): Backend class for interacting with the backend.

    Returns:
        str: JSON with the whole metadata for a table.
    """
    if table_id == "__all__":
        table_ids = []
        tables = backend._get_tables_for_dataset(dataset_id)["tables"]
        for table in tables:
            for cloudtable in table["cloudTables"]:
                table_ids.append(cloudtable["gcpTableId"])
    else:
        table_ids = [table_id]
    metadatas = []
    for table_id in table_ids:
        print(f"Building metadata for `{dataset_id}.{table_id}`...")
        metadatas.append(backend.get_table_config(dataset_id, table_id))
    return metadatas


def merge_metadatas(metadatas: List[Dict[str, Any]]):
    """
    Merges all metadata into one single metadata. Inputs must be in the following format:
    {
        "slug": "...",
        "name": "...",
        "description": "...",
        "dataset": {
            "slug": "...",
            "name": "...",
            "description": "...",
            "themes": [
                {
                    "slug": "...",
                    "name": "..."
                }
            ],
            "tags": [
                {
                    "slug": "...",
                    "name": "..."
                }
            ],
        }
        "status": {
            "name": "...",
            "slug": "..."
        },
        "license": {
            "name": "...",
            "slug": "..."
        },
        "pipeline": {
            "githubUrl": "..."
        },
        "publishedBy": {
            "firstName": "...",
            "lastName": "...",
            "email": "..."
        },
        "dataCleanedBy": {
            "firstName": "...",
            "lastName": "...",
            "email": "..."
        },
        "dataCleaningDescription",
        "dataCleaningCodeUrl",
        "rawDataUrl",
        "auxiliaryFilesUrl",
        "architectureUrl",
    }

    We must merge so it looks like this (dataset outside, table inside):
    {
        "dataset_slug": {
            "name": "...",
            "description": "...",
            "themes": [
                {
                    "slug": "...",
                    "name": "..."
                }
            ],
            "tags": [
                {
                    "slug": "...",
                    "name": "..."
                }
            ],
            "tables": [
                {
                    "slug": "...",
                    "name": "...",
                    "description": "...",
                    "status": {
                        "name": "...",
                        "slug": "..."
                    },
                    "license": {
                        "name": "...",
                        "slug": "..."
                    },
                    "pipeline": {
                        "githubUrl": "..."
                    },
                    "publishedBy": {
                        "firstName": "...",
                        "lastName": "...",
                        "email": "..."
                    },
                    "dataCleanedBy": {
                        "firstName": "...",
                        "lastName": "...",
                        "email": "..."
                    },
                    "dataCleaningDescription",
                    "dataCleaningCodeUrl",
                    "rawDataUrl",
                    "auxiliaryFilesUrl",
                    "architectureUrl",
                }
            ]
        }
    }
    """
    final_metadata = {}
    for metadata in metadatas:
        dataset_slug = metadata["dataset"]["slug"]
        if dataset_slug not in final_metadata:
            dataset_metadata = deepcopy(metadata["dataset"])
            del dataset_metadata["slug"]
            dataset_metadata["tables"] = []
            final_metadata[dataset_slug] = dataset_metadata
        table_metadata = deepcopy(metadata)
        del table_metadata["dataset"]
        final_metadata[dataset_slug]["tables"].append(table_metadata)
    return final_metadata


def update_metadata_json(
    final_metadata: Dict[str, Any], deleted_datasets_tables: List[Tuple[str, str]]
) -> None:
    """
    Gets the latest version of metadata.json file, if exists, and updates it with the new metadata.
    """
    try:
        with open("metadata.json", "r") as f:
            metadata = json.load(f)
    except FileNotFoundError:
        metadata = {}

    # Now we remove the deleted datasets and tables
    for dataset_slug, table_slug in deleted_datasets_tables:
        # If the table_slug is __all__, we remove the whole dataset
        if table_slug == "__all__":
            if dataset_slug in metadata:
                print(f"Removing dataset `{dataset_slug}`...")
                del metadata[dataset_slug]
        # Otherwise, we remove only the table
        else:
            print(f"Removing table `{dataset_slug}.{table_slug}`...")
            if dataset_slug in metadata:
                metadata_dataset_tables = [
                    table["slug"] for table in metadata[dataset_slug]["tables"]
                ]
                if table_slug in metadata_dataset_tables:
                    metadata[dataset_slug]["tables"] = [
                        table
                        for table in metadata[dataset_slug]["tables"]
                        if table["slug"] != table_slug
                    ]

    # For each dataset slug
    for dataset_slug, dataset_metadata in final_metadata.items():
        # If it's not yet on the file, simply add it
        if dataset_slug not in metadata:
            print(f"Adding dataset `{dataset_slug}`...")
            metadata[dataset_slug] = dataset_metadata
        # If it's already there
        else:
            # Grab the list of table IDs for that dataset from the file
            metadata_dataset_tables = [
                table["slug"] for table in metadata[dataset_slug]["tables"]
            ]
            # For each table in the new metadata
            for table in dataset_metadata["tables"]:
                print(f"Adding/updating table `{dataset_slug}.{table['slug']}`...")
                # If it's not yet on the file, simply add it
                if table["slug"] not in metadata_dataset_tables:
                    metadata[dataset_slug]["tables"].append(table)
                # If it's already there
                else:
                    # Iterate over the tables in the file and update the one that matches
                    for metadata_table in metadata[dataset_slug]["tables"]:
                        if metadata_table["slug"] == table["slug"]:
                            metadata_table.update(table)

    # Write the new metadata to the file
    with open("metadata.json", "w") as f:
        json.dump(metadata, f, indent=4)


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
        modified_files, show_details=True
    )

    # Split deleted datasets and tables
    deleted_datasets_tables = []
    existing_datasets_tables = []
    for dataset_id, table_id, exists, _ in datasets_tables:
        if exists:
            existing_datasets_tables.append((dataset_id, table_id))
        else:
            deleted_datasets_tables.append((dataset_id, table_id))
    print(f"Deleted datasets and tables: {deleted_datasets_tables}")
    print(f"Existing datasets and tables: {existing_datasets_tables}")

    # Initialize backend
    backend = Backend(args.graphql_url)

    # Build metadatas for each table
    metadatas = []
    for dataset_id, table_id in existing_datasets_tables:
        metadata = build_metadata(dataset_id, table_id, backend)
        metadatas.extend(metadata)

    # Merge metadatas
    final_metadata = merge_metadatas(metadatas)

    # Update metadata.json file
    update_metadata_json(final_metadata, deleted_datasets_tables)
