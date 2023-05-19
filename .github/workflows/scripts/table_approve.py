from argparse import ArgumentParser
import sys
from time import sleep
import traceback

import basedosdados as bd
from basedosdados import Dataset, Storage

from backend import Backend
from utils import expand_alls, get_datasets_tables_from_modified_files


def get_flow_run_state(flow_run_id: str, backend: Backend, auth_token: str):
    query = """
    query ($flow_run_id: uuid!) {
        flow_run_by_pk (id: $flow_run_id) {
            state
        }
    }
    """
    response = backend._execute_query(
        query,
        variables={"flow_run_id": flow_run_id},
        headers={"Authorization": f"Bearer {auth_token}"},
    )
    return response["flow_run_by_pk"]["state"]


def get_materialization_flow_id(backend: Backend, auth_token: str):
    query = """
    query {
        flow (where: {
            name: {
                _like: "BD template: Executa DBT model"
            },
            archived: {
                _eq: false
            },
            project: {
                name: {_eq: "main"}
            }
        }) {
            id
        }
    }
    """
    response = backend._execute_query(
        query, headers={"Authorization": f"Bearer {auth_token}"}
    )
    return response["flow"][0]["id"]


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

    # Add source bucket name argument
    arg_parser.add_argument(
        "--source-bucket-name",
        type=str,
        required=True,
        help="Source bucket name.",
    )

    # Add destination bucket name argument
    arg_parser.add_argument(
        "--destination-bucket-name",
        type=str,
        required=True,
        help="Destination bucket name.",
    )

    # Add backup bucket name argument
    arg_parser.add_argument(
        "--backup-bucket-name",
        type=str,
        required=True,
        help="Backup bucket name.",
    )

    # Add Prefect backend URL argument
    arg_parser.add_argument(
        "--prefect-backend-url",
        type=str,
        required=False,
        default="https://prefect.basedosdados.org/api",
        help="Prefect backend URL.",
    )

    # Add prefect base URL argument
    arg_parser.add_argument(
        "--prefect-base-url",
        type=str,
        required=False,
        default="https://prefect.basedosdados.org",
        help="Prefect base URL.",
    )

    # Add Prefect API token argument
    arg_parser.add_argument(
        "--prefect-backend-token",
        type=str,
        required=True,
        help="Prefect backend token.",
    )

    # Add materialization mode argument
    arg_parser.add_argument(
        "--materialization-mode",
        type=str,
        required=False,
        default="prod",
        help="Materialization mode.",
    )

    # Add materialization label argument
    arg_parser.add_argument(
        "--materialization-label",
        type=str,
        required=True,
        help="Materialization label.",
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
    for dataset_id, table_id, exists, alias in datasets_tables:
        if exists:
            existing_datasets_tables.append((dataset_id, table_id, alias))
        else:
            deleted_datasets_tables.append((dataset_id, table_id, alias))

    # Expand `__all__` tables
    backend = Backend(args.graphql_url)
    expanded_existing_datasets_tables = []
    for dataset_id, table_id, alias in existing_datasets_tables:
        expanded_table_ids = expand_alls(dataset_id, table_id, backend)
        for expanded_dataset_id, expanded_table_id in expanded_table_ids:
            expanded_existing_datasets_tables.append(
                (expanded_dataset_id, expanded_table_id, alias)
            )
    existing_datasets_tables = expanded_existing_datasets_tables

    # # Sync and create tables
    # for dataset_id, table_id, _ in existing_datasets_tables:
    #     print(f"Creating table {dataset_id}.{table_id}...")
    #     push_table_to_bq(
    #         dataset_id=dataset_id,
    #         table_id=table_id,
    #         source_bucket_name=args.source_bucket_name,
    #         destination_bucket_name=args.destination_bucket_name,
    #         backup_bucket_name=args.backup_bucket_name,
    #     )
    #     print(f"Table {dataset_id}.{table_id} created.")

    # Launch materialization flows
    backend = Backend(args.prefect_backend_url)
    flow_id = get_materialization_flow_id(backend, args.prefect_backend_token)
    launched_flow_run_ids = []
    for dataset_id, table_id, alias in existing_datasets_tables:
        print(
            f"Launching materialization flow for {dataset_id}.{table_id} (alias={alias})..."
        )
        parameters = {
            "dataset_id": dataset_id,
            "dbt_alias": alias,
            "mode": args.materialization_mode,
            "table_id": table_id,
        }
        mutation = """
        mutation ($flow_id: UUID, $parameters: JSON, $label: String!) {
            create_flow_run (input: {
                flow_id: $flow_id,
                parameters: $parameters,
                labels: [$label],
            }) {
                id
            }
        }
        """
        variables = {
            "flow_id": flow_id,
            "parameters": parameters,
            "label": args.materialization_label,
        }
        response = backend._execute_query(
            mutation,
            variables,
            headers={"Authorization": f"Bearer {args.prefect_backend_token}"},
        )
        flow_run_id = response["create_flow_run"]["id"]
        launched_flow_run_ids.append(flow_run_id)
        flow_run_url = f"{args.prefect_base_url}/flow-run/{flow_run_id}"
        print(f" - Materialization flow run launched: {flow_run_url}")

    # Keep monitoring the launched flow runs until they are finished
    for launched_flow_run_id in launched_flow_run_ids:
        print(f"Monitoring flow run {launched_flow_run_id}...")
        flow_run_state = get_flow_run_state(
            flow_run_id=launched_flow_run_id,
            backend=backend,
            auth_token=args.prefect_backend_token,
        )
        while flow_run_state not in ["Success", "Failed", "Cancelled"]:
            sleep(5)
            flow_run_state = get_flow_run_state(
                flow_run_id=launched_flow_run_id,
                backend=backend,
                auth_token=args.prefect_backend_token,
            )
        if flow_run_state != "Success":
            raise Exception(
                f'Flow run {launched_flow_run_id} finished with state "{flow_run_state}". '
                f"Check the logs at {args.prefect_base_url}/flow-run/{launched_flow_run_id}"
            )
        print(f"Flow run {launched_flow_run_id} finished successfully.")
