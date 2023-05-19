from argparse import ArgumentParser
import sys
import traceback

import basedosdados as bd
from basedosdados import Dataset, Storage

from backend import Backend
from utils import expand_alls, get_datasets_tables_from_modified_files


def push_table_to_bq(
    dataset_id,
    table_id,
    source_bucket_name="basedosdados-dev",
    destination_bucket_name="basedosdados",
    backup_bucket_name="basedosdados-backup",
):
    # copy proprosed data between storage buckets
    # create a backup of old data, then delete it and copies new data into the destination bucket
    # modes = ["staging", "raw", "auxiliary_files", "architecture", "header"]
    modes = ["staging"]

    for mode in modes:
        try:
            sync_bucket(
                source_bucket_name=source_bucket_name,
                dataset_id=dataset_id,
                table_id=table_id,
                destination_bucket_name=destination_bucket_name,
                backup_bucket_name=backup_bucket_name,
                mode=mode,
            )
            print()
        except Exception as error:
            print(f"DATA ERROR ON {mode}.{dataset_id}.{table_id}")
            traceback.print_exc(file=sys.stderr)
            print()

    # create table object of selected table and dataset ID
    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)

    # delete table from staging and prod if exists
    tb.delete(mode="staging")

    # create the staging table in bigquery
    tb.create(
        path=f"./staging/{dataset_id}/{table_id}/",
        if_table_exists="replace",
        if_storage_data_exists="pass",
        dataset_is_public=True,
    )

    # updates the dataset description
    Dataset(dataset_id).update(mode="prod")


def sync_bucket(
    source_bucket_name: str,
    dataset_id: str,
    table_id: str,
    destination_bucket_name: str,
    backup_bucket_name: str,
    mode: str = "staging",
) -> None:
    """Copies proprosed data between storage buckets.
    Creates a backup of old data, then delete it and copies new data into the destination bucket.

    Args:
        source_bucket_name (str):
            The bucket name from which to copy data.
        dataset_id (str):
            Dataset id available in basedosdados. It should always come with table_id.
        table_id (str):
            Table id available in basedosdados.dataset_id.
            It should always come with dataset_id.
        destination_bucket_name (str):
            The bucket name which data will be copied to.
            If None, defaults to the bucket initialized when instantianting Storage object
            (check it with the Storage.bucket proprerty)
        backup_bucket_name (str):
            The bucket name for where backup data will be stored.
        mode (str): Optional
            Folder of which dataset to update.[raw|staging|header|auxiliary_files|architecture]

    Raises:
        ValueError:
            If there are no files corresponding to the given dataset_id and table_id on the source bucket
    """

    ref = Storage(dataset_id=dataset_id, table_id=table_id)

    prefix = f"{mode}/{dataset_id}/{table_id}/"

    source_ref = (
        ref.client["storage_staging"]
        .bucket(source_bucket_name)
        .list_blobs(prefix=prefix)
    )

    destination_ref = ref.bucket.list_blobs(prefix=prefix)

    if len(list(source_ref)) == 0:
        raise ValueError(
            f"No objects found on the source bucket {source_bucket_name}.{prefix}"
        )

    if len(list(destination_ref)):
        backup_bucket_blobs = list(
            ref.client["storage_staging"]
            .bucket(backup_bucket_name)
            .list_blobs(prefix=prefix)
        )
        if len(backup_bucket_blobs):
            print(f"{mode.upper()}: DELETE BACKUP DATA")
            ref.delete_table(
                not_found_ok=True, mode=mode, bucket_name=backup_bucket_name
            )

        print(f"{mode.upper()}: BACKUP OLD DATA")
        ref.copy_table(
            source_bucket_name=destination_bucket_name,
            destination_bucket_name=backup_bucket_name,
            mode=mode,
        )

        print(f"{mode.upper()}: DELETE OLD DATA")
        ref.delete_table(
            not_found_ok=True, mode=mode, bucket_name=destination_bucket_name
        )

    print(f"{mode.upper()}: TRANSFER NEW DATA")
    ref.copy_table(
        source_bucket_name=source_bucket_name,
        destination_bucket_name=destination_bucket_name,
        mode=mode,
    )
    if mode == "staging":
        print("DOWNLOADING FIRST BLOB DATA TO LOCAL")
        blobs = (
            ref.client["storage_staging"]
            .bucket("basedosdados-dev")
            .list_blobs(prefix=f"staging/{dataset_id}/{table_id}/")
        )
        ## only needs the first bloob
        for blob in blobs:
            blob_path = str(blob.name).replace(f"staging/{dataset_id}/{table_id}/", "")
            ref.download(filename=blob_path, savepath=".", mode="staging")
            break


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
    print(existing_datasets_tables)

    # Sync and create tables
    for dataset_id, table_id in existing_datasets_tables:
        print(f"Creating table {dataset_id}.{table_id}...")
        push_table_to_bq(
            dataset_id=dataset_id,
            table_id=table_id,
            source_bucket_name="basedosdados-dev",  # TODO: replace
            destination_bucket_name="basedosdados",  # TODO: replace
            backup_bucket_name="basedosdados-backup",  # TODO: replace
        )
        print(f"Table {dataset_id}.{table_id} created.")
